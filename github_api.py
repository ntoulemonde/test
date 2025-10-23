import requests
import os
import re

def get_commit_message(commit_sha):
    url = f"https://api.github.com/repos/InseeFrLab/ssphub/commits/{commit_sha}"
    token = os.environ['GIT_PERSONAL_ACCESS_TOKEN']
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        commit_data = response.json()
        return commit_data['commit']['message']
    else:
        raise Exception(f"Failed to fetch commit message: {response.status_code}")

# Example usage
print(get_commit_message("2780577afbbee6153193eeaa3cd5120f9c0774db"))


def process_commit_messages(text):
    # Regular expression to find commit SHAs
    commit_sha_pattern = re.compile(r'- (\w+)')

    # Find all commit SHAs in the text
    commit_shas = commit_sha_pattern.findall(text)

    # Process each commit SHA
    for commit_sha in commit_shas:
        try:
            commit_message = get_commit_message(commit_sha)
            # Replace the commit SHA with the commit SHA and message
            text = text.replace(f"- {commit_sha}", f"- {commit_sha}: {commit_message}")
        except Exception as e:
            print(f"Failed to fetch commit message for {commit_sha}: {e}")

    return text

# Example usage
text = """
Issue also adressed in commits :

- 2780577afbbee6153193eeaa3cd5120f9c0774db 
- 2d881e5f663fb588b4fc5b490279fdf680a007ba
- 7dc2a04938df589b29a2ff99600eaae79a9a58fc
- 9b37cb7f4d7468ccee70c35aeef287b3a5bd716e
- ca6573e2ff5c0dfe773d08852b6948ffcaa27d78
- acd65447a34392e214247b45333a27fd228256f4
- dfbd0da12cb2b7d374593e9660470582241035bd
- 2ab90ab3e1c9ad64d427df0f6c4d6eddf10d9083
- af9285d39e5dc3644a61f2ec826b52a48a0f030e
- 8162baa4732e0fcd8c7d996df3c05916fd3d204b
- d5f1c9e4fcbe1e17bf1553b46de7c7eb7726f221
- 78aa4e4fd8b094c6e4dd226a2dfd5347f9493f53
- 4b2bb7b955c767ab3f10cbd62909160cc343a0af
- 67334240aa24f21923ab3e4749e2f2ac94e94b46
- 510c79929748a5259211625f0cebcc3d26fd0fb0
"""
print(process_commit_messages(text))

