import os
import requests

PREDICATE_KEYWORDS = ["predicate", "biolink:", "edges"]
LABEL_NAME = "Biological Context QC"  # Label to add if keywords are found

# GitHub API variables
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = os.getenv("GITHUB_REPOSITORY")
ISSUE_NUMBER = os.getenv("ISSUE_NUMBER")
print("GITHUB_TOKEN:", GITHUB_TOKEN)
print("REPO_NAME:", REPO_NAME)
print("ISSUE_NUMBER:", ISSUE_NUMBER)

headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
api_url = f"https://api.github.com/repos/{REPO_NAME}"

def get_issue_details(issue_number):
    response = requests.get(f"{api_url}/issues/{issue_number}", headers=headers)
    response.raise_for_status()
    return response.json()

def add_label(issue_number, label_name):
    response = requests.post(
        f"{api_url}/issues/{issue_number}/labels",
        headers=headers,
        json={"labels": [label_name]}
    )
    response.raise_for_status()
    print(f"Label '{label_name}' added to issue/PR #{issue_number}")

def check_keywords_in_text(text, keywords):
    return any([keyword in text for keyword in keywords]) if text else False

def main():
    issue_details = get_issue_details(ISSUE_NUMBER)
    title = issue_details["title"]
    body = issue_details["body"]

    if check_keywords_in_text(title, PREDICATE_KEYWORDS) or check_keywords_in_text(body, PREDICATE_KEYWORDS):
        add_label(ISSUE_NUMBER, LABEL_NAME)
    else:
        print("No predicate keywords found.")

if __name__ == "__main__":
    main()
