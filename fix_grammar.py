import os
from openai import OpenAI
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    for root, dirs, files in os.walk("./docs/website/docs", topdown=False):
        for name in files:
            if not name.endswith(".md"):
                continue

            filename = os.path.join(root, name)
            with open(filename, "r") as f:
                doc = f.read()
            
            print("fixing grammar for", filename)
            client = OpenAI()

            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": """
You are a grammar checker. Every message you get will be a document that is to be grammarchecked and returned as such.
You will not change the markdown syntax. You will only fix the grammar. You will not change the code snippets except for code snippets therein.
You will not modify the header section which is enclosed by two occurences of "---". 
Do not change the spelling or casing of these words: dlt, sdf, dbt
"""},
                    {"role": "user", "content": doc},
                ],
                temperature=0,
            )

            fixed_doc = response.choices[0].message.content

            fixed_doc = fixed_doc + """
<!---
grammarcheck: true
-->
"""

            with open(filename, "w") as f:
                f.writelines(fixed_doc)

