import os
import pendulum
from openai import OpenAI
from dotenv import load_dotenv

CHECK_TOKEN = "last-grammar-check"
BASE_DIR = "./docs/website/docs"

if __name__ == "__main__":
    load_dotenv()
    for root, dirs, files in os.walk(BASE_DIR, topdown=False):
        for name in files:
            if not name.endswith(".md"):
                continue

            if not name == "running.md":
                continue

            filename = os.path.join(root, name)
            with open(filename, "r") as f:
                doc = f.readlines()

            # find the grammar check token
            last_checked = None
            for l in doc:
                if CHECK_TOKEN in l:
                    date = l.split(":", 1)[1].strip()
                    last_checked = date
                    break

            # for now always skip if was run once, can be modify to run after offset later
            if last_checked:
                print(f"skipping {filename}, last checked {date}")
                continue

            # insert grammar check marker, but only if header exists
            if "---" in doc[0]:
                doc.insert(1, f"{CHECK_TOKEN}: {pendulum.now().to_iso8601_string()}\n")
            
            print("fixing grammar for", filename)
            client = OpenAI()

            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": """
You are a grammar checker. Every message you get will be a document that is to be grammarchecked and returned as such.
You will not change the markdown syntax. You will only fix the grammar. You will not change the code snippets except for the comments therein.
You will not modify the header section which is enclosed by two occurences of "---". 
Do not change the spelling or casing of these words: dlt, sdf, dbt
"""},
                    {"role": "user", "content": "".join(doc)},
                ],
                temperature=0,
            )

            fixed_doc = response.choices[0].message.content
            

            with open(filename, "w") as f:
                f.writelines(fixed_doc)
