import json
import os
import logging
from dotenv import load_dotenv
from litellm import completion
import litellm

async def call_chatcompletion_api(prompt, model):
    load_dotenv()
    os.environ['GEMINI_API_KEY'] = os.getenv("GEMINI_KEY")
    litellm.enable_json_schema_validation = True
    model_name = "gemini/gemini-1.5-flash"
    try:
        response = completion(
            model=model_name, 
            messages=[
            {"role": "user", "content": prompt}
        ],
        response_format=model,
        )
        logging.info(f"Response: {response}")
        # Check response structure
        if hasattr(response, "usage"):
            token_counts = {
                "input_tokens": response.usage.prompt_tokens,
                "output_tokens": response.usage.completion_tokens,
            }
        else:
            token_counts = None
        # Parse output
        try:
            output = json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            output = response.choices[0].message.content
        
        return output, token_counts
    except Exception as e:
        logging.error(f"Error during API call: {e}")
        return None, None
