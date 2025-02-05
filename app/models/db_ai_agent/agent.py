import ollama

import yaml
import json

from app.database import DBHelper
from app.models.db_ai_agent.query_executor import SqlQueryExecutor


class DbAiAgent:
    def __init__(self, config=None):
        if config is None:
            with open("./app/config.yml", "r") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = config

        self.db_manager = DBHelper()
        self.messages = [{"role": "system", "content": self.get_system_prompt()}]
        self.ollama_model = self.config["DB_AGENT_MODEL"]
        self.query_executor = SqlQueryExecutor(
            db_manager=self.db_manager, config=config
        )
        self.history = []

    def get_schema(self):
        """Retrieve and return the schema of the database."""
        return self.db_manager.get_schema()

    def get_system_prompt(self) -> str:
        """Generate SQL query from a plain English query using Ollama."""
        schema = self.db_manager.get_schema()
        schema_prompt = self.create_schema_narrative(schema)

        # System-level prompt that explains the task to Ollama
        system_prompt = f"""
        You are an AI agent with a strong understanding of both SQL and human language. Your task is to generate SQL queries based on the schema of tables provided in the database. You will receive a user's request in natural language, and based on the schema information you have, you need to construct an accurate SQL query.

        Here is the schema information for the database tables:

        {schema_prompt}

        Your responsibilities:
        1. **Understand the user's request**: Read and comprehend the request provided by the user in natural language.
        2. **Identify the relevant tables and columns**: Based on the request, determine which tables and columns should be used.
        3. **Generate the appropriate SQL query**: Construct a correct and efficient SQL query that meets the user's request.
        4. **Handle ambiguities**: If the user's request is unclear, ask for clarifications before generating the SQL query.
        
        You will respond in a JSON structure that indicates whether you are providing the SQL query or need more information from the user.
        The structure is as follows:
        ```
        {{"needs_clarification": true/false,
        "query": "SQL_QUERY_OR_NULL",
        "clarification_needed": "QUESTION_OR_NULL"
        }}
        ```

        ### Example:

        **User's Request**:
        "I need to find the names and email addresses of all customers who made a purchase in the last 30 days."

        **Expected JSON Response**:
            ```
            {{"needs_clarification": false,
            "query": "SELECT c.name, c.email FROM customers c JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_date &gt;= NOW() - INTERVAL 30 DAY;",
            "clarification_needed": null
            }}
        ```

        ### Example when clarification is needed:

        **User's Request**:
        "I need to know the orders from the last month."

        **Expected JSON Response**:
        ```
        {{"needs_clarification": true,
        "query": null,
        "clarification_needed": "Do you want to retrieve all orders from the last month, or are you looking for specific details such as customer names or product details?"
        }}
        ```

        Follow these guidelines to ensure accuracy and efficiency:
        1. **Comprehension**: Ensure you fully understand the user's request before constructing the query.
        2. **Accuracy**: The generated SQL query must be accurate, reflect the user's request, and adhere to the provided schema.
        3. **Efficiency**: Write queries that are optimized and efficient.
        4. **Error Handling**: If the user's request is unclear, ask for clarification before generating the SQL query.

        Always provide a clear and concise JSON response based on the user's request and the database schema provided.
        """

        # Pass the system-level prompt to Ollama model for query generation
        return system_prompt

    def create_schema_narrative(self, schema: dict) -> str:
        """Create a detailed narrative describing the schema of the database."""
        narrative = "Here is the schema of the database:\n"
        for table, columns in schema.items():
            narrative += f"\nTable `{table}` has the following columns:\n"
            for column in columns:
                narrative += f"  - {column}\n"
            narrative += "\n"
        return narrative

    def run_model(self, message_list):
        response = ollama.chat(model=self.ollama_model, messages=message_list)
        response = response.message.content
        json_start = response.index("{")
        json_end = len(response) - 1
        while json_end >= 0 and response[json_end] != "}":
            json_end -= 1

        response = json.loads(response[json_start : json_end + 1])
        return response

    def get_system_prompt2(self):
        return """
    You are an AI agent with a strong understanding of SQL and human language. Your task is to provide plain English explanations for the results obtained from running SQL queries. You will receive the user's request, the SQL query that was run, and the result of the query. Based on this information, you need to construct a clear and easy-to-understand explanation for the end user.

    Here is how you should proceed:

    1. Receive the input: You will be given:
        - The user's request in natural language.
        - The SQL query that was run.
        - The result of the query in a tabular format (e.g., list of dictionaries).
        
    2. Understand the context: Comprehend the user's request, the purpose of the query, and the significance of the results.

    3. Generate the explanation: Construct a clear and concise explanation in plain English that addresses the user's request and describes the query results
    
    ### Guidelines:
    1. **Comprehension**: Make sure you fully understand the user's request and the query result before constructing the explanation.
    2. **Clarity**: Use clear and concise language to explain the query result in layman's terms.
    3. **Context**: Provide context if necessary to help the user understand the significance of the results.
    4. **Error Handling**: If the inputs are unclear, provide a plain English explanation of what is missing or needs clarification.
    """

    def handle_user_query(self, query: str):
        """Process the user query and return the result."""
        new_message = [{"role": "user", "content": query}]
        message_list = self.messages + self.history + new_message
        response = self.run_model(message_list=message_list)
        if str(response["needs_clarification"]).lower() == "true":
            self.history += [{"role": "user", "content": query}] + [
                {"role": "assistant", "content": response["clarification_needed"]}
            ]
            return response["clarification_needed"]
        else:
            output = self.query_executor.execute_sql_query(response["query"])
            message_list = (
                [
                    {
                        "role": "system",
                        "content": self.get_system_prompt2(),
                    }
                ]
                + new_message
                + [
                    {
                        "role": "assistant",
                        "content": f"Output after executing query '{response['query']}' -> {output}",
                    }
                ]
            )
            response = ollama.chat(model=self.ollama_model, messages=message_list)
            self.history = []
            return response.message.content

    def close(self):
        """Close the database connection."""
        self.db_manager.close()
