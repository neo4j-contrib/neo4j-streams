import json
import os
import requests
import sys


def upload_asset(asset_name, release_id, headers):
    with open(asset_name, "rb") as file_name_handle:
        upload_url = "https://uploads.github.com/repos/neo4j-contrib/neo4j-streams/releases/{release_id}/assets?name={asset_name}".format(
            release_id=release_id, asset_name=asset_name.split("/")[-1]
        )
    print(upload_url)
    headers["Content-Type"] = "application/java-archive"
    response = requests.post(upload_url, headers=headers, data=file_name_handle.read())
    print(response.text)


def main(token, tag_name, plugin_file_name, kafka_connect_file_name):
    headers = {"Authorization": "bearer {token}".format(token=token)}
    data = {'tag_name': tag_name}
    response = requests.post("https://api.github.com/repos/neo4j-contrib/neo4j-streams/releases",
                             data=json.dumps(data), headers=headers)
    release_json = response.json()
    print(release_json)
    release_id = release_json["id"]

    upload_asset(plugin_file_name, release_id, headers)
    upload_asset(kafka_connect_file_name, release_id, headers)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python release.py [tag_name] [plugin_file_name] [kafka_connect_file_name]")
        sys.exit(1)

    token = os.getenv("GITHUB_TOKEN")
    tag_name = sys.argv[1]
    plugin_file_name = sys.argv[2]
    kafka_connect_file_name = sys.argv[3]
    main(token, tag_name, plugin_file_name, kafka_connect_file_name)
