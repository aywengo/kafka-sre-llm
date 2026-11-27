"""Edit a Kubernetes object read as JSON on stdin; print it on stdout (for `kubectl replace -f -`).

    jsonedit.py env NAME VALUE       set env NAME of the first container to a plain VALUE (drops valueFrom);
                                     also sets the Recreate strategy
    jsonedit.py unset-env NAME       remove env NAME of the first container
"""
import json
import sys


def main(argv):
    obj = json.load(sys.stdin)
    obj["metadata"].pop("resourceVersion", None)
    c = obj["spec"]["template"]["spec"]["containers"][0]
    env = [e for e in c.get("env", []) if e["name"] != argv[1]]
    if argv[0] == "env":
        env.insert(0, {"name": argv[1], "value": argv[2]})
        # Recreate: the old, healthy pod goes away with the change, as after an operator restart; with a rolling
        # update it would keep working next to the broken new pod and the fault would show no symptom
        obj["spec"]["strategy"] = {"type": "Recreate"}
    c["env"] = env
    json.dump(obj, sys.stdout)


if __name__ == "__main__":
    main(sys.argv[1:])
