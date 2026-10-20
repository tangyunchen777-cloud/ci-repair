import asyncio

from app.models import QueueEnvelope


class SQSQueue:
    def __init__(self, client, queue_url: str):
        self.client, self.queue_url = client, queue_url

    async def publish(self, envelope: QueueEnvelope) -> str:
        response = await asyncio.to_thread(
            self.client.send_message, QueueUrl=self.queue_url, MessageBody=envelope.model_dump_json(),
        )
        return response["MessageId"]

    async def receive(self) -> dict | None:
        response = await asyncio.to_thread(
            self.client.receive_message, QueueUrl=self.queue_url,
            MaxNumberOfMessages=1, WaitTimeSeconds=20, VisibilityTimeout=300,
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
        )
        return next(iter(response.get("Messages", [])), None)

    async def delete(self, receipt_handle: str):
        await asyncio.to_thread(self.client.delete_message, QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
