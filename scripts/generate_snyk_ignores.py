import json
import os
import sys


def find_ignore_list(snyk_output_path):
    with open(snyk_output_path) as snyk_output:
        json_string = snyk_output.read()
        snyk = json.loads(json_string)

        critical_severity = {}
        high_severity = {}
        medium_severity = {}
        low_severity = {}
        for project in snyk:
            vulnerabilities = project["vulnerabilities"]
            if len(vulnerabilities) > 0:
                for vulnerability in vulnerabilities:
                    if vulnerability["severity"] == "high":
                        high_severity[vulnerability["id"]] = vulnerability["title"]
                    elif vulnerability["severity"] == "critical":
                        critical_severity[vulnerability["id"]] = vulnerability["title"]
                    elif vulnerability["severity"] == "medium":
                        medium_severity[vulnerability["id"]] = vulnerability["title"]
                    elif vulnerability["severity"] == "low":
                        low_severity[vulnerability["id"]] = vulnerability["title"]

        print("Critical severity vulnerabilities found: " + str(len(critical_severity)))
        print("High severity vulnerabilities found: " + str(len(high_severity)))
        print("Medium severity vulnerabilities found: " + str(len(medium_severity)))
        print("Low severity vulnerabilities found: " + str(len(low_severity)))

        print("critical_severity: " + json.dumps(critical_severity, indent=2))
        print("high_severity: " + json.dumps(high_severity, indent=2))
        print("medium_severity: " + json.dumps(medium_severity, indent=2))
        print("low_severity: " + json.dumps(low_severity, indent=2))
        return critical_severity.items(), high_severity.items()


def find_replace(ignore_items):
    for key, value in ignore_items:
        os.system("snyk ignore --id=" + key + " --reason=\"" + value + "\" --expiry=2100-01-01")


def main(args):
    criticals, highs = find_ignore_list(args[1])
    find_replace(criticals)
    find_replace(highs)


if __name__ == '__main__':
    main(sys.argv)
