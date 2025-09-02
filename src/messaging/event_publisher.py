import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, date
import pika
from config.settings import settings

logger = logging.getLogger(__name__)

class RabbitMQEventPublisher:
    """Service để publish events tới RabbitMQ"""

    def __init__(self):
        self.connection = None
        self.channel = None
        self._setup_connection()

    def _setup_connection(self):
        """Thiết lập kết nối RabbitMQ"""
        try:
            # RabbitMQ connection parameters
            connection_params = pika.ConnectionParameters(
                host=settings.rabbitmq.host,
                port=settings.rabbitmq.port,
                virtual_host=settings.rabbitmq.virtual_host,
                credentials=pika.PlainCredentials(
                    settings.rabbitmq.username,
                    settings.rabbitmq.password
                ) if settings.rabbitmq.username else None,
                heartbeat=600,
                blocked_connection_timeout=300,
            )

            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()

            # Declare exchanges và queues
            self._setup_exchanges_and_queues()

            logger.info("RabbitMQ connection established successfully")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            self.connection = None
            self.channel = None

    def _setup_exchanges_and_queues(self):
        """Thiết lập exchanges và queues"""
        try:
            # Declare exchange cho appointment events
            self.channel.exchange_declare(
                exchange='prescription_exchange',
                exchange_type='topic',
                durable=True
            )

            # Declare queue cho confirmed appointments
            self.channel.queue_declare(
                queue='prescription_notifications',
                durable=True,
                arguments={'x-message-ttl': 86400000}  # TTL 1 hour
            )

            # Bind queues với routing keys
            self.channel.queue_bind(
                exchange='prescription_exchange',
                queue='prescription_notifications',
                routing_key='prescription.ready'
            )

            logger.info("RabbitMQ exchanges and queues setup completed")

        except Exception as e:
            logger.error(f"Failed to setup RabbitMQ exchanges/queues: {e}")
            raise

    def publish_ready_prescription(self, prescription_data: Dict[str, Any]):
        if not self._is_connection_healthy():
            logger.warning("RabbitMQ connection unhealthy, attempting to reconnect...")
            self._setup_connection()

        if not self.channel:
            logger.error("Cannot publish event: RabbitMQ connection not available")
            return False
        try:
            event_message={
                "event_type": "prescription_ready",
                "timestamp": datetime.now().isoformat(),
                "source_service": "prescription-management",
                "data": {
                    "prescription_id": prescription_data["prescription_id"],
                    "prescription_code": prescription_data["prescription_code"],
                    "appointment_id": prescription_data["appointment_id"],
                    "dispense_id": prescription_data["dispense_id"]                }
            }
            self.channel.basic_publish(
                exchange=settings.rabbitmq.exchange_name,
                routing_key=settings.rabbitmq.routing_key,
                body=json.dumps(event_message, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    message_id=f"prescription_ready_{prescription_data['prescription_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
            )

            logger.info(f"Published prescription confirmed event for prescription_id: {prescription_data['prescription_id']}")
            return True

        except Exception as e:
            logger.error(f"Failed to publish prescription ready event: {e}")
            return False

    def _is_connection_healthy(self) -> bool:
        """Kiểm tra connection RabbitMQ có healthy không"""
        try:
            return (self.connection and
                    not self.connection.is_closed and
                    self.channel and
                    not self.channel.is_closed)
        except Exception:
            return False

    def close(self):
        """Đóng connection RabbitMQ"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")

    def __del__(self):
        """Destructor để đảm bảo connection được đóng"""
        self.close()

_event_publisher: Optional[RabbitMQEventPublisher] = None
def get_event_publisher() -> RabbitMQEventPublisher:
    """Get singleton event publisher instance"""
    global _event_publisher
    if _event_publisher is None:
        _event_publisher = RabbitMQEventPublisher()
    return _event_publisher