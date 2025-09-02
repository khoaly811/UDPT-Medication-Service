import uvicorn
from config import settings

if __name__ == "__main__":
    print("RabbitMQ host:", settings.rabbitmq.host)
    print("RabbitMQ user:", settings.rabbitmq.username)
    print("RabbitMQ exchange:", settings.rabbitmq.exchange_name)
    print("RabbitMQ queue:", settings.rabbitmq.queue_name)
    uvicorn.run(
        "src.controllers.front_controller:app",
        host=settings.app.host,
        port=settings.app.port,
        reload=settings.app.reload
    )