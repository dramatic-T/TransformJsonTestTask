from github import Github
import json
import os


class RepositoryExtractor:
    def __init__(self):
        self.__github = Github()

    def extract(self, organization_name: str):
        org = self.__github.get_organization(organization_name)

        if not os.path.exists("repositories"):
            os.makedirs("repositories")

        for repo in org.get_repos():

            repo_data = {
                "repo": {
                    "full_name": repo.full_name,
                    "name": repo.name,
                    "id": repo.id,
                    "owner": {
                        "login": repo.owner.login,
                        "url": repo.owner.html_url
                    }
                },
                "pull_requests": [self.__build_pr_dict(pr) for pr in repo.get_pulls(state='all')]
            }

            with open(f"repositories/{repo.name}.json", "w") as file:
                json.dump(repo_data, file, indent=4)

    @staticmethod
    def __build_pr_dict(pr):
        return {
            "number": pr.number,
            "title": pr.title,
            "state": pr.state,
            "created_at": pr.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "merged_at": pr.merged_at.strftime("%Y-%m-%d %H:%M:%S") if pr.merged_at else None
        }