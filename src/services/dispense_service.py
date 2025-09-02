from decimal import Decimal
from typing import List

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from src.repositories.dispense_repository import DispenseRepository
from src.repositories.prescription_repository import PrescriptionRepository
from src.models.dispense import Dispense, DispenseLine
from src.dto.dispense_dto import (
    DispenseCreateDTO, DispenseLineCreateDTO, DispenseCompleteDTO,
    DispenseResponseDTO, DispenseLineResponseDTO
)
from src.messaging.event_publisher import get_event_publisher


class DispenseService:
    def __init__(self, db: Session):
        self.repo = DispenseRepository(db)
        self.prescription_repo = PrescriptionRepository(db)
        self.event_publisher = get_event_publisher()

    # 1) Tạo phiếu cấp phát (PENDING)
    def create_dispense(self, dto: DispenseCreateDTO) -> DispenseResponseDTO:
        pres = self.prescription_repo.get_prescription(dto.prescription_id)
        if not pres:
            raise HTTPException(status_code=404, detail="Prescription not found")
        if pres.status == "DISPENSED":
            raise HTTPException(status_code=400, detail=f"Cannot create dispense for prescription in status {pres.status}")

        entity = Dispense(
            prescription_id=dto.prescription_id,
            notes=dto.notes,
        )
        created = self.repo.create_dispense(entity)
        return self._to_dispense_response(created, [])

    # 2) Thêm dòng cấp phát
    def add_line(self, dispense_id: int, dto: DispenseLineCreateDTO) -> DispenseResponseDTO:
        dispense = self.repo.get_dispense(dispense_id)
        if not dispense:
            raise HTTPException(status_code=404, detail="Dispense not found")
        if dispense.status != "PENDING":
            raise HTTPException(status_code=400, detail="Only PENDING dispense can accept lines")

        pi = self.prescription_repo.get_prescription_item(dto.prescription_item_id)
        if not pi:
            raise HTTPException(status_code=404, detail="Prescription item not found")
        if pi.prescription_id != dispense.prescription_id:
            raise HTTPException(status_code=400, detail="Prescription item does not belong to this prescription")

        if dto.quantity_dispensed <= 0:
            raise HTTPException(status_code=400, detail="quantity_dispensed must be > 0")

        already_completed = self.repo.sum_completed_dispensed_for_item(pi.item_id)
        already_in_this   = self.repo.sum_lines_in_dispense_for_item(dispense.dispense_id, pi.item_id)
        total_after       = Decimal(already_completed) + Decimal(already_in_this) + Decimal(dto.quantity_dispensed)

        if total_after > pi.quantity_prescribed:
            raise HTTPException(
                status_code=400,
                detail=f"Dispensed total ({total_after}) exceeds prescribed ({pi.quantity_prescribed}) for this item"
            )

        med = self.repo.get_medicine(pi.medication_id)
        if med and med.stock is not None and Decimal(med.stock) < Decimal(dto.quantity_dispensed):
            raise HTTPException(status_code=400, detail="Insufficient stock for this medicine at the moment")

        line = DispenseLine(
            dispense_id=dispense.dispense_id,
            prescription_item_id=pi.item_id,
            quantity_dispensed=dto.quantity_dispensed,
            notes=dto.notes
        )
        self.repo.add_line(line)

        lines = self.repo.get_lines_by_dispense(dispense.dispense_id)
        return self._to_dispense_response(dispense, lines)

    # 3) Hoàn tất phiếu
    def complete(self, dispense_id: int, dto: DispenseCompleteDTO) -> DispenseResponseDTO:
        dispense = self.repo.get_dispense(dispense_id)
        if not dispense:
            raise HTTPException(status_code=404, detail="Dispense not found")
        if dispense.status != "PENDING":
            raise HTTPException(status_code=400, detail="Only PENDING dispense can be completed")

        lines = self.repo.get_lines_by_dispense(dispense.dispense_id)
        if not lines:
            raise HTTPException(status_code=400, detail="Cannot complete an empty dispense")

        try:
            self.repo.bulk_check_and_decrement_stock_for_dispense(dispense.dispense_id)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        completed = self.repo.complete_dispense(dispense, dto.dispensed_by)

        pres = self.prescription_repo.get_prescription(dispense.prescription_id)
        if self._is_prescription_fully_dispensed(pres.prescription_id):
            self.prescription_repo.set_prescription_status(pres, "DISPENSED")
        elif self._is_prescription_partially_dispensed(pres.prescription_id):
            self.prescription_repo.set_prescription_status(pres, "PARTIALLY_DISPENSED")

        self.repo.commit()

        try:
            publisher = get_event_publisher()
            publisher.publish_ready_prescription({
                "prescription_id": pres.prescription_id,
                "prescription_code": pres.prescription_code,
                "appointment_id": pres.appointment_id,
                "dispense_id": dispense.dispense_id
            })
        except Exception as e:
            print(f"⚠️ RabbitMQ publish failed: {e}")

        final_lines = self.repo.get_lines_by_dispense(dispense.dispense_id)
        return self._to_dispense_response(completed, final_lines)

    # 4) Detail
    def get_dispense(self, dispense_id: int) -> DispenseResponseDTO:
        dispense = self.repo.get_dispense(dispense_id)
        if not dispense:
            raise HTTPException(status_code=404, detail="Dispense not found")
        lines = self.repo.get_lines_by_dispense(dispense.dispense_id)
        return self._to_dispense_response(dispense, lines)

    # ---- Helpers ----
    def _is_prescription_fully_dispensed(self, prescription_id: int) -> bool:
        items = self.prescription_repo.get_prescription_items(prescription_id)
        if not items:
            return False
        for it in items:
            completed_qty = self.repo.sum_completed_dispensed_for_item(it.item_id)
            if Decimal(completed_qty) < Decimal(it.quantity_prescribed):
                return False
        return True

    def _is_prescription_partially_dispensed(self, prescription_id: int) -> bool:
        items = self.prescription_repo.get_prescription_items(prescription_id)
        if not items:
            return False
        any_dispensed = False
        all_fully = True
        for it in items:
            completed_qty = self.repo.sum_completed_dispensed_for_item(it.item_id)
            if Decimal(completed_qty) > 0:
                any_dispensed = True
            if Decimal(completed_qty) < Decimal(it.quantity_prescribed):
                all_fully = False

        return any_dispensed and not all_fully

    def _to_dispense_response(self, dispense: Dispense, lines: List[DispenseLine]) -> DispenseResponseDTO:
        line_dtos = [DispenseLineResponseDTO.model_validate(l) for l in lines]
        return DispenseResponseDTO(
            dispense_id=dispense.dispense_id,
            prescription_id=dispense.prescription_id,
            status=dispense.status,
            dispensed_at=dispense.dispensed_at,
            dispensed_by=dispense.dispensed_by,
            notes=dispense.notes,
            lines=line_dtos
        )
