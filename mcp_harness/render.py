"""Check the downloaded manifests against downloads.lock.json and write the harness's versions of them.

    .venv/bin/python mcp_harness/render.py RAW_DIR OUT_DIR

Strimzi 1.3.1 (install YAML): namespace `myproject` -> `kafka`, STRIMZI_IMAGE_PULL_POLICY=IfNotPresent, small
requests; the operator Deployment is also written on its own (operator-deployment.yaml) so `reset` can replace it.
Argo CD v3.6.1 (core-install.yaml): every imagePullPolicy -> IfNotPresent, reconciliation every 30 s.
Offline; no network.
"""
import hashlib
import json
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_lock(raw):
    lock = json.loads((HERE / "downloads.lock.json").read_text())
    out = {}
    for m in lock["manifests"]:
        p = Path(raw) / Path(m["path"]).name
        got = sha256(p)
        if got != m["sha256"]:
            raise SystemExit(f"render: {p.name} sha256 {got} does not match the lock {m['sha256']}")
        out[m["name"]] = p
    return out


def pull_policy(obj):
    """Set imagePullPolicy IfNotPresent on every container and init container of a workload."""
    spec = (obj.get("spec") or {}).get("template", {}).get("spec")
    if not spec:
        return
    for key in ("containers", "initContainers"):
        for c in spec.get(key) or []:
            c["imagePullPolicy"] = "IfNotPresent"


def strimzi(src):
    docs = [d for d in yaml.safe_load_all(Path(src).read_text()) if d]
    deployment = None
    for d in docs:
        if d.get("kind") in ("RoleBinding", "ClusterRoleBinding"):
            for s in d.get("subjects") or []:
                if s.get("namespace") == "myproject":
                    s["namespace"] = "kafka"
        if d.get("metadata", {}).get("namespace") == "myproject":
            d["metadata"]["namespace"] = "kafka"
        if d.get("kind") == "Deployment" and d["metadata"]["name"] == "strimzi-cluster-operator":
            d["metadata"]["namespace"] = "kafka"
            pull_policy(d)
            c = d["spec"]["template"]["spec"]["containers"][0]
            c["env"] = [e for e in c["env"] if e["name"] != "STRIMZI_IMAGE_PULL_POLICY"]
            for e in c["env"]:  # a shorter operation timeout, so a stuck rolling update gives up in 2 minutes
                if e["name"] == "STRIMZI_OPERATION_TIMEOUT_MS":
                    e["value"] = "120000"
            c["env"].append({"name": "STRIMZI_IMAGE_PULL_POLICY", "value": "IfNotPresent"})
            c["resources"] = {"requests": {"cpu": "100m", "memory": "384Mi"}, "limits": {"memory": "512Mi"}}
            deployment = d
    if deployment is None:
        raise SystemExit("render: no strimzi-cluster-operator Deployment in the Strimzi YAML")
    return docs, deployment


# Argo CD v3.6.1 has no built-in health check for Strimzi topics and users; this one (a documented
# argocd-cm customization) reads their Ready condition, so a topic the operator rejects makes the app Degraded.
_READY_LUA = """hs = {status = "Progressing", message = "Waiting for the Ready condition"}
if obj.status ~= nil and obj.status.conditions ~= nil then
  for _, c in ipairs(obj.status.conditions) do
    if c.type == "Ready" and c.status == "True" then hs.status = "Healthy"; hs.message = "Ready" end
    if c.type == "Ready" and c.status == "False" then hs.status = "Degraded"; hs.message = c.message or c.reason or "Not ready" end
  end
end
return hs
"""
STRIMZI_HEALTH = {f"resource.customizations.health.kafka.strimzi.io_{k}": _READY_LUA for k in ("KafkaTopic", "KafkaUser")}


def argocd(src):
    docs = [d for d in yaml.safe_load_all(Path(src).read_text()) if d]
    for d in docs:
        if d.get("kind") in ("Deployment", "StatefulSet"):
            pull_policy(d)
        if d.get("kind") == "ConfigMap" and d["metadata"]["name"] == "argocd-cm":
            d["data"] = dict(d.get("data") or {}, **{"timeout.reconciliation": "30s"}, **STRIMZI_HEALTH)
        if d.get("kind") == "ConfigMap" and d["metadata"]["name"] == "argocd-cmd-params-cm":
            # Argo CD 3 keeps resource health out of the Application status by default; the read-only tools
            # read the status through the Kubernetes API, so it is kept there.
            d["data"] = dict(d.get("data") or {}, **{"controller.resource.health.persist": "true"})
    return docs


def dump(docs, path):
    Path(path).write_text(yaml.safe_dump_all(docs, sort_keys=False))


def main(argv):
    raw, out = Path(argv[0]), Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    found = check_lock(raw)
    docs, dep = strimzi(found["strimzi-cluster-operator"])
    dump(docs, out / "strimzi-operator.yaml")
    dump([dep], out / "operator-deployment.yaml")
    dump(argocd(found["argo-cd-core-install"]), out / "argocd-core.yaml")
    print(f"render: manifests match the lock; wrote {out}/strimzi-operator.yaml, operator-deployment.yaml, argocd-core.yaml")


if __name__ == "__main__":
    main(sys.argv[1:])
