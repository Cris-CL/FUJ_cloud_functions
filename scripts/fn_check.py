#!/usr/bin/env python3
"""Read-only drift check: deploy.sh + local folder vs the live Cloud Function (Gen 2).

Usage: python3 scripts/fn_check.py <function-name> <folder>

Config: runs <folder>/deploy.sh with GCLOUD=echo, parses the flags and compares them with
`gcloud functions describe`. Source: downloads the deployed source zip and compares it with the
local files that `.gcloudignore` does not exclude.

Exit codes: 0 both match, 1 config drift (or error), 2 source drift only.
Standard library only; never writes to GCP.
"""
import difflib
import fnmatch
import json
import os
import shlex
import subprocess
import sys
import tempfile
import zipfile

IGNORED_ENV = {"LOG_EXECUTION_ID"}  # added by the console, not part of deploy.sh

# Gen 2 CPU when --cpu is not passed, by memory (gcloud/Cloud Functions defaults).
DEFAULT_CPU = {256: 0.1666, 512: 0.3333, 1024: 0.5833, 2048: 1.0, 4096: 2.0, 8192: 2.0, 16384: 4.0, 32768: 8.0}
DEFAULT_MAX_INSTANCES = 100

VALUE_FLAGS = {
    "--project", "--region", "--runtime", "--source", "--entry-point", "--trigger-bucket",
    "--trigger-location", "--trigger-topic", "--service-account", "--trigger-service-account",
    "--memory", "--cpu", "--timeout", "--max-instances", "--min-instances", "--concurrency",
    "--set-env-vars", "--set-secrets", "--ingress-settings",
}
BOOL_FLAGS = {"--gen2", "--retry", "--no-retry", "--trigger-http", "--allow-unauthenticated",
              "--no-allow-unauthenticated"}


def run(cmd, **kw):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kw).stdout


def parse_deploy(folder):
    out = run(["bash", os.path.join(folder, "deploy.sh")], env={**os.environ, "GCLOUD": "echo"})
    tokens = shlex.split(out)
    if tokens[:2] != ["functions", "deploy"]:
        raise SystemExit(f"deploy.sh did not print a 'functions deploy' command: {out!r}")
    name, flags, unknown = tokens[2], {}, []
    i = 3
    while i < len(tokens):
        tok = tokens[i]
        key, _, inline = tok.partition("=")
        if key in VALUE_FLAGS:
            if inline:
                flags[key] = inline
            else:
                flags[key] = tokens[i + 1]
                i += 1
        elif tok in BOOL_FLAGS:
            flags[tok] = True
        else:
            unknown.append(tok)
        i += 1
    return name, flags, unknown


def mem_mib(value):
    v = value.strip()
    units = {"Ki": 1 / 1024, "Mi": 1, "Gi": 1024, "M": 1e6 / 2**20, "G": 1e9 / 2**20, "MB": 1, "GB": 1024}
    for u in sorted(units, key=len, reverse=True):
        if v.endswith(u):
            return round(float(v[: -len(u)]) * units[u])
    return round(int(v) / 2**20)


def cpu_float(value):
    v = str(value)
    return float(v[:-1]) / 1000 if v.endswith("m") else float(v)


def seconds(value):
    v = str(value)
    if v.endswith("s"):
        v = v[:-1]
    return int(v)


def kv_list(value):
    out = {}
    for part in value.split(","):
        if part:
            k, _, v = part.partition("=")
            out[k] = v
    return out


def check_config(fn, flags, unknown, live, project_number, invoker_public):
    drift = []
    sc, bc, et = live["serviceConfig"], live["buildConfig"], live.get("eventTrigger")
    default_sa = f"{project_number}-compute@developer.gserviceaccount.com"

    def cmp(label, want, have):
        if want != have:
            drift.append(f"{label}: deploy.sh={want!r} live={have!r}")

    for tok in unknown:
        drift.append(f"unrecognised token in deploy.sh (not checked): {tok}")
    if not flags.get("--gen2"):
        drift.append("deploy.sh does not pass --gen2")
    cmp("runtime", flags.get("--runtime"), bc.get("runtime"))
    cmp("entry point", flags.get("--entry-point"), bc.get("entryPoint"))
    cmp("source", flags.get("--source"), ".")

    # Trigger
    if flags.get("--trigger-http"):
        if et:
            drift.append(f"trigger: deploy.sh=HTTP live={et.get('eventType')}")
        want_public = bool(flags.get("--allow-unauthenticated"))
        cmp("allUsers invoker", want_public, invoker_public)
    elif "--trigger-bucket" in flags:
        if not et or et.get("eventType") != "google.cloud.storage.object.v1.finalized":
            drift.append(f"trigger: deploy.sh=GCS finalize live={et and et.get('eventType')}")
        else:
            bucket = {f["attribute"]: f["value"] for f in et.get("eventFilters", [])}.get("bucket")
            cmp("trigger bucket", flags["--trigger-bucket"], bucket)
    elif "--trigger-topic" in flags:
        if not et or et.get("eventType") != "google.cloud.pubsub.topic.v1.messagePublished":
            drift.append(f"trigger: deploy.sh=Pub/Sub live={et and et.get('eventType')}")
        else:
            cmp("trigger topic", flags["--trigger-topic"], et.get("pubsubTopic", "").rsplit("/", 1)[-1])
    else:
        drift.append("deploy.sh has no trigger flag")
    if et:
        cmp("trigger location", flags.get("--trigger-location", flags.get("--region")), et.get("triggerRegion"))
        cmp("retry", "RETRY_POLICY_RETRY" if flags.get("--retry") else "RETRY_POLICY_DO_NOT_RETRY",
            et.get("retryPolicy"))
        cmp("trigger service account", flags.get("--trigger-service-account", default_sa),
            et.get("serviceAccountEmail"))

    # Service
    cmp("service account", flags.get("--service-account", default_sa), sc.get("serviceAccountEmail"))
    memory = mem_mib(flags.get("--memory", "256Mi"))
    cmp("memory (MiB)", memory, mem_mib(sc.get("availableMemory", "256Mi")))
    want_cpu = cpu_float(flags["--cpu"]) if "--cpu" in flags else DEFAULT_CPU.get(memory)
    have_cpu = cpu_float(sc.get("availableCpu", "0"))
    if want_cpu is None or abs(want_cpu - have_cpu) > 0.002:
        src = "--cpu" if "--cpu" in flags else f"default for {memory}MiB"
        drift.append(f"cpu: deploy.sh={want_cpu} ({src}) live={have_cpu}")
    cmp("timeout (s)", seconds(flags.get("--timeout", "60")), sc.get("timeoutSeconds"))
    cmp("max instances", int(flags.get("--max-instances", DEFAULT_MAX_INSTANCES)), sc.get("maxInstanceCount"))
    cmp("min instances", int(flags.get("--min-instances", 0)), sc.get("minInstanceCount", 0))
    cmp("concurrency", int(flags.get("--concurrency", 1)), sc.get("maxInstanceRequestConcurrency", 1))
    ingress = {"all": "ALLOW_ALL", "internal-only": "ALLOW_INTERNAL_ONLY",
               "internal-and-gclb": "ALLOW_INTERNAL_AND_GCLB"}
    cmp("ingress", ingress.get(flags.get("--ingress-settings", "all")), sc.get("ingressSettings"))

    want_env = kv_list(flags.get("--set-env-vars", ""))
    have_env = {k: v for k, v in (sc.get("environmentVariables") or {}).items() if k not in IGNORED_ENV}
    for k in sorted(set(want_env) | set(have_env)):
        cmp(f"env {k}", want_env.get(k), have_env.get(k))

    want_sec = kv_list(flags.get("--set-secrets", ""))
    have_sec = {s["key"]: f"{s['secret']}:{s['version']}" for s in sc.get("secretEnvironmentVariables") or []}
    for k in sorted(set(want_sec) | set(have_sec)):
        cmp(f"secret {k}", want_sec.get(k), have_sec.get(k))
    return drift


def load_ignore(folder):
    path = os.path.join(folder, ".gcloudignore")
    if not os.path.exists(path):
        return None
    pats = []
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#"):
            pats.append(line)
    return pats


def ignored(rel, is_dir, pats):
    """Simplified .gcloudignore (gitignore) matching: enough for the patterns used in this repo."""
    parts = rel.split("/")
    for p in pats:
        if p.startswith("!"):
            continue  # negations are not used here
        dir_only = p.endswith("/")
        pat = p.rstrip("/").lstrip("/")
        if dir_only and not is_dir:
            continue
        if "/" in pat:
            if fnmatch.fnmatch(rel, pat):
                return True
        elif fnmatch.fnmatch(parts[-1], pat):
            return True
    return False


def local_files(folder, pats):
    out = {}
    for dirpath, dirnames, filenames in os.walk(folder):
        reldir = os.path.relpath(dirpath, folder)
        reldir = "" if reldir == "." else reldir + "/"
        dirnames[:] = [d for d in dirnames if not ignored(reldir + d, True, pats)]
        for f in filenames:
            rel = reldir + f
            if not ignored(rel, False, pats):
                with open(os.path.join(dirpath, f), "rb") as fh:
                    out[rel] = fh.read()
    return out


def deployed_files(src, tmp):
    zpath = os.path.join(tmp, "src.zip")
    uri = f"gs://{src['bucket']}/{src['object']}#{src['generation']}"
    run(["gcloud", "storage", "cp", uri, zpath, "--quiet"])
    with zipfile.ZipFile(zpath) as z:
        return {n: z.read(n) for n in z.namelist() if not n.endswith("/")}


def check_source(folder, live):
    drift = []
    pats = load_ignore(folder)
    if pats is None:
        return ["no .gcloudignore in the folder"]
    src = live["buildConfig"]["source"].get("storageSource")
    if not src:
        return [f"deployed source is not a storage zip: {live['buildConfig']['source']}"]
    with tempfile.TemporaryDirectory() as tmp:
        remote = deployed_files(src, tmp)
    local = local_files(folder, pats)
    for name in sorted(set(remote) - set(local)):
        drift.append(f"only deployed: {name}")
    for name in sorted(set(local) - set(remote)):
        drift.append(f"only local:    {name}")
    for name in sorted(set(local) & set(remote)):
        if local[name] != remote[name]:
            try:
                a = remote[name].decode().splitlines()
                b = local[name].decode().splitlines()
                diff = list(difflib.unified_diff(a, b, f"deployed/{name}", f"local/{name}", lineterm="", n=1))
                body = "\n".join("      " + d for d in diff[:40])
                more = f"\n      ... {len(diff) - 40} more diff lines" if len(diff) > 40 else ""
                drift.append(f"differs:       {name}\n{body}{more}")
            except UnicodeDecodeError:
                drift.append(f"differs:       {name} (binary)")
    return drift


def main():
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    fn, folder = sys.argv[1], sys.argv[2]
    name, flags, unknown = parse_deploy(folder)
    if name != fn:
        raise SystemExit(f"deploy.sh deploys {name!r}, not {fn!r}")
    project, region = flags.get("--project"), flags.get("--region")
    if not project or not region:
        raise SystemExit("deploy.sh must pass --project and --region")
    live = json.loads(run(["gcloud", "functions", "describe", fn, "--project", project,
                           "--region", region, "--format=json"]))
    project_number = live["buildConfig"]["source"]["storageSource"]["bucket"].split("-")[3] \
        if live["buildConfig"].get("source", {}).get("storageSource") else \
        run(["gcloud", "projects", "describe", project, "--format=value(projectNumber)"]).strip()
    invoker_public = None
    if not live.get("eventTrigger"):
        service = live["serviceConfig"]["service"].rsplit("/", 1)[-1]
        policy = json.loads(run(["gcloud", "run", "services", "get-iam-policy", service, "--project", project,
                                 "--region", region, "--format=json"]))
        invoker_public = any(b.get("role") == "roles/run.invoker" and "allUsers" in b.get("members", [])
                             for b in policy.get("bindings", []))

    print(f"{fn}  ({folder}, {project}/{region})")
    config = check_config(fn, flags, unknown, live, project_number, invoker_public)
    print("CONFIG OK" if not config else "CONFIG DRIFT")
    for d in config:
        print("  - " + d)
    source = check_source(folder, live)
    print("SOURCE IDENTICAL" if not source else "SOURCE DRIFT")
    for d in source:
        print("  - " + d)
    sys.exit(1 if config else 2 if source else 0)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"command failed: {' '.join(e.cmd)}\n{e.stderr}")
        sys.exit(1)
