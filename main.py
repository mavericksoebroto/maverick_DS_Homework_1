
import os
import asyncio
import pandas as pd
from dotenv import load_dotenv
import io

# Import AI Agent modules
from autogen_agentchat.agents import AssistantAgent, UserProxyAgent
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.messages import TextMessage
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_ext.agents.web_surfer import MultimodalWebSurfer

# Load environment variables (API key)
load_dotenv()

async def process_chunk(chunk, start_idx, total_records, model_client, termination_condition):
    """
    Processes a batch of cryptocurrency data:
    - Converts data into dictionary format.
    - Generates prompts for AI agents to analyze the batch.
    - Uses MultimodalWebSurfer to search for additional cryptocurrency information.
    - Collects and returns all responses.
    """
    print(f"Processing chunk from {start_idx} to {start_idx + len(chunk) - 1}...")
    chunk_data = chunk.to_dict(orient='records')
    prompt = (
        f"Processing records {start_idx} to {start_idx + len(chunk) - 1} (total {total_records} records).\n"
        f"Batch data:\n{chunk_data}\n\n"
        "Please analyze the data and provide cryptocurrency recommendations.\n"
        " 1. Assess Bitcoin trends and market performance.\n"
        " 2. Use MultimodalWebSurfer to fetch external crypto market insights.\n"
        " 3. Provide clear recommendations and references."
    )

    # Create AI agent team
    data_agent = AssistantAgent("data_agent", model_client)
    web_surfer = MultimodalWebSurfer("web_surfer", model_client)
    assistant = AssistantAgent("assistant", model_client)
    user_proxy = UserProxyAgent("user_proxy")
    team = RoundRobinGroupChat(
        [data_agent, web_surfer, assistant, user_proxy],
        termination_condition=termination_condition
    )

    messages = []
    async for event in team.run_stream(task=prompt):
        if isinstance(event, TextMessage):
            print(f"[{event.source}] => {event.content}\n")
            messages.append({
                "batch_start": start_idx,
                "batch_end": start_idx + len(chunk) - 1,
                "source": event.source,
                "content": event.content,
            })
    return messages

async def main():
    # Load API Key
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
    if not GEMINI_API_KEY:
        print("Error: API Key missing. Check your .env file.")
        return

    print("API Key loaded successfully.")

    # Initialize OpenAI API Client
    model_client = OpenAIChatCompletionClient(
        model="gemini-2.0-flash",
        api_key=GEMINI_API_KEY,
    )

    termination_condition = TextMentionTermination("exit")

    # Read data in chunks
    csv_file_path = "crypto_data.csv"
    try:
        chunk_size = 1000
        chunks = list(pd.read_csv(csv_file_path, chunksize=chunk_size, encoding="utf-8-sig", skip_blank_lines=True, quotechar='"'))
        total_records = sum(chunk.shape[0] for chunk in chunks)
        print(f"Data loaded successfully with {total_records} total records.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Process each batch in parallel
    tasks = [process_chunk(chunk, i * chunk_size, total_records, model_client, termination_condition) for i, chunk in enumerate(chunks)]
    results = await asyncio.gather(*tasks)

    # Flatten the results
    all_messages = [msg for batch in results for msg in batch]

    # Debug: Check if any messages were collected
    print(f"Collected messages: {all_messages}")

    if all_messages:
        # Save results
        try:
            df_log = pd.DataFrame(all_messages)
            output_file = "all_conversation_log.csv"
            df_log.to_csv(output_file, index=False, encoding="utf-8-sig")
            pd.DataFrame(all_messages).to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"Results saved to {output_file}.")
        except Exception as e:
            print(f"Error saving output: {e}")
    else:
        print("No messages collected. Check the agent processing.")

if __name__ == '__main__':
    print("Starting the process...")
    asyncio.run(main())
