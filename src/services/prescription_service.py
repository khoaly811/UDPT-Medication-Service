import os

from sqlalchemy.orm import Session
from fastapi import HTTPException, status
import httpx
from typing import List
from src.repositories.prescription_repository import PrescriptionRepository
from src.models.prescription import Prescription, PrescriptionItem
from src.dto.pagination_dto import PaginatedResponseDTO, PaginationRequestDTO
from src.dto.prescription_dto import (
    PrescriptionCreateDTO,
    PrescriptionResponseDTO,
    PrescriptionItemResponseDTO,
    PrescriptionListItemDTO,
    PrescriptionUpdateDTO,
    PrescriptionItemAddDTO,
    CancelPrescriptionDTO,
)

APPOINTMENT_SERVICE_URL = str(os.getenv("APPOINTMENT__SERVICE__ENDPOINT", "http://localhost:8005"))

class PrescriptionService:
    def __init__(self, db: Session):
        self.repository = PrescriptionRepository(db)

    def _validate_appointment_exists(self, appointment_id: int):
        try:
            url = f"{APPOINTMENT_SERVICE_URL}/appointments/{appointment_id}"
            resp = httpx.get(url, timeout=5.0)

            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Appointment not found")
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Appointment service error")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Error contacting Appointment service: {e}")
    # ---------------- Helpers ----------------
    def _to_response(self, prescription: Prescription) -> PrescriptionResponseDTO:
        items_in_db = self.repository.get_items_by_prescription_id(prescription.prescription_id)
        item_dtos = [PrescriptionItemResponseDTO.model_validate(it) for it in items_in_db]
        return PrescriptionResponseDTO(
            prescription_id=prescription.prescription_id,
            prescription_code=prescription.prescription_code,
            appointment_id=prescription.appointment_id,
            status=prescription.status,
            valid_from=prescription.valid_from,
            valid_to=prescription.valid_to,
            notes=prescription.notes,
            created_at=prescription.created_at,
            created_by=prescription.created_by,
            updated_at=prescription.updated_at,
            updated_by=prescription.updated_by,
            canceled_at=prescription.canceled_at,
            canceled_by=prescription.canceled_by,
            canceled_reason=prescription.canceled_reason,
            items=item_dtos
        )

    def create_prescription(self, data: PrescriptionCreateDTO) -> PrescriptionResponseDTO:
        self._validate_appointment_exists(data.appointment_id)

        if not data.items or len(data.items) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Prescription must have at least one item"
            )

        prescription = Prescription(
            appointment_id=data.appointment_id,
            valid_from=data.valid_from,
            valid_to=data.valid_to,
            notes=data.notes,
            created_by=data.created_by,
            status="CREATED",
        )

        items = [
            PrescriptionItem(
                medication_id=item.medication_id,
                quantity_prescribed=item.quantity_prescribed,
                unit_prescribed=item.unit_prescribed,
                dose=item.dose,
                frequency=item.frequency,
                duration=item.duration,
                notes=item.notes,
            )
            for item in data.items
        ]

        created = self.repository.create_prescription(prescription, items)
        return self._to_response(created)

    def get_prescription_by_id(self, prescription_id: int) -> PrescriptionResponseDTO:
        prescription = self.repository.get_prescription_by_id(prescription_id)
        if not prescription:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Prescription with id {prescription_id} not found"
            )
        return self._to_response(prescription)

    def list_prescriptions(self, pagination: PaginationRequestDTO) -> PaginatedResponseDTO[PrescriptionListItemDTO]:
        skip = pagination.calc_skip()
        rows = self.repository.get_all_prescriptions(skip=skip, limit=pagination.page_size)
        total = self.repository.count_prescriptions()
        data = [PrescriptionListItemDTO.model_validate(r) for r in rows]
        return PaginatedResponseDTO.create(
            data=data,
            page=pagination.page,
            page_size=pagination.page_size,
            total=total
        )

    def update_prescription(self, prescription_id: int, data: PrescriptionUpdateDTO) -> PrescriptionResponseDTO:
        prescription = self.repository.get_prescription_by_id(prescription_id)
        if not prescription:
            raise HTTPException(status_code=404, detail="Prescription not found")
        if prescription.status == "CANCELED":
            raise HTTPException(status_code=400, detail="Cannot edit a canceled prescription")

        if data.valid_from is not None:
            prescription.valid_from = data.valid_from
        if data.valid_to is not None:
            prescription.valid_to = data.valid_to
        if data.notes is not None:
            prescription.notes = data.notes
        if data.updated_by is not None:
            prescription.updated_by = data.updated_by

        prescription.status = "UPDATED"
        updated = self.repository.update_prescription(prescription)
        return self._to_response(updated)

    def add_item(self, prescription_id: int, dto: PrescriptionItemAddDTO) -> PrescriptionResponseDTO:
        prescription = self.repository.get_prescription_by_id(prescription_id)
        if not prescription:
            raise HTTPException(status_code=404, detail="Prescription not found")
        if prescription.status == "CANCELED":
            raise HTTPException(status_code=400, detail="Cannot add item to a canceled prescription")

        item = PrescriptionItem(
            medication_id=dto.medication_id,
            quantity_prescribed=dto.quantity_prescribed,
            unit_prescribed=dto.unit_prescribed,
            dose=dto.dose,
            frequency=dto.frequency,
            duration=dto.duration,
            notes=dto.notes,
        )
        self.repository.add_item(prescription_id, item)
        return self.get_prescription_by_id(prescription_id)

    def remove_item(self, prescription_id: int, item_id: int) -> PrescriptionResponseDTO:
        prescription = self.repository.get_prescription_by_id(prescription_id)
        if not prescription:
            raise HTTPException(status_code=404, detail="Prescription not found")
        if prescription.status == "CANCELED":
            raise HTTPException(status_code=400, detail="Cannot remove item from a canceled prescription")

        # kiểm tra tồn tại
        item = self.repository.get_prescription_item(item_id)
        if not item or item.prescription_id != prescription_id:
            raise HTTPException(status_code=404, detail="Prescription item not found")

        deleted = self.repository.remove_item_by_id(prescription_id, item_id)
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Prescription item not found")

        prescription.status = "UPDATED"
        self.repository.update_prescription(prescription)

        return self.get_prescription_by_id(prescription_id)

    def cancel_prescription(self, prescription_id: int, data: CancelPrescriptionDTO) -> PrescriptionResponseDTO:
        prescription = self.repository.get_prescription_by_id(prescription_id)
        if not prescription:
            raise HTTPException(status_code=404, detail="Prescription not found")
        if prescription.status == "CANCELED":
            return self._to_response(prescription)

        canceled = self.repository.cancel_prescription(
            prescription,
            reason=data.reason,
            canceled_by=data.canceled_by
        )
        return self._to_response(canceled)
