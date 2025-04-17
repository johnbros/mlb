import requests

video_url = "https://sporty-clips.mlb.com/cWU0bDFfVjBZQUhRPT1fQjFVQ1VRWlFYZ1FBREZVTEJBQUFBUUFBQUFBQ1ZGQUFVVllEQTFCV0FWVlhVd0JS.mp4"
output_file = "downloads/1dc5bd86-479b-406f-a771-c363cc0f2c27.mp4"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://baseballsavant.mlb.com/"
}

response = requests.get(video_url, headers=headers, stream=True)

if response.status_code == 200:
    with open(output_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"[SUCCESS] Video saved to {output_file}")
else:
    print(f"[ERROR] Download failed. Status code: {response.status_code}")
