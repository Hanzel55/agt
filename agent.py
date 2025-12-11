import grpc
import asyncio
import json
import decimal
import app_pb2
import app_pb2_grpc
from db_utils import connect_to_postgres, connect_to_sql_server


AGENT_ID = "agent-123"
SERVER_ADDRESS = "localhost:50051"


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Decimal objects."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)


async def sender(stream, send_queue):
    """Background task: sends messages to server."""
    while True:
        msg = await send_queue.get()
        await stream.write(msg)


async def receiver(stream, send_queue):
    """Receives messages from server and enqueues responses."""
    async for server_msg in stream:
        print(f"[AGENT] Received from server: {server_msg}")

        if server_msg.query and server_msg.database:
            print("[AGENT] Executing DB query...")
            req_id = server_msg.request_id 
            print(server_msg.tbl_name,"[[[[[[[]]]]]]]")
            # returns list-of-dicts already JSON-safe
            if server_msg.database == "postgres":
                error, records , column_metadata = connect_to_postgres(server_msg.query)
            elif server_msg.database == "sqlserver":
                error, records , column_metadata = connect_to_sql_server(server_msg.query,server_msg.tbl_name)
            else:
                print("[AGENT] Unsupported database type")
                continue
            try:
                response = app_pb2.AgentMessage(
                    message=error,
                    query_result = app_pb2.QueryResult(
                        json=json.dumps(records, cls=DecimalEncoder),
                        request_id=req_id,
                        tbl_meta_data=json.dumps(column_metadata)
                    )
                )
            except Exception as e:
                print("[AGENT] Error:", e)
                continue

            print("[AGENT] Sending query result...")
            await send_queue.put(response)


async def run_agent():

    while True:
        try:
            async with grpc.aio.insecure_channel(SERVER_ADDRESS) as channel:
                stub = app_pb2_grpc.AgntServiceStub(channel)

                print("[AGENT] Connecting to server…")

                # Create outgoing message stream using a queue
                send_queue = asyncio.Queue()

                # Start bidirectional stream
                stream = stub.Connect()

                # 1️⃣ Send agent_id
                print("[AGENT] Sending agent_id...")
                await send_queue.put(app_pb2.AgentMessage(agent_id=AGENT_ID))

                # 2️⃣ Start sender task
                sender_task = asyncio.create_task(sender(stream, send_queue))

                # 3️⃣ Start receiver task
                receiver_task = asyncio.create_task(receiver(stream, send_queue))

                # 4️⃣ Start heartbeat loop
                async def heartbeat():
                    while True:
                        await asyncio.sleep(5)
                        print("[AGENT] Sending heartbeat...")
                        await send_queue.put(app_pb2.AgentMessage(heartbeat="alive"))

                heartbeat_task = asyncio.create_task(heartbeat())

                # wait for all tasks
                await asyncio.gather(sender_task, receiver_task, heartbeat_task)
        except grpc.aio.AioRpcError as e:
            print("\n[AGENT]  Connection lost or server offline:", e)
            print("[AGENT]  Retrying in 5 seconds...\n")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run_agent())
