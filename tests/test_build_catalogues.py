"""Parsers of scripts/build_catalogues.py on small inline fixtures (no raw/ needed)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import build_catalogues as bc  # noqa: E402

HTML = """<ul class="config-list">
<li>
<h4><a id="max.poll.interval.ms"></a><a id="consumerconfigs_max.poll.interval.ms" href="#x">max.poll.interval.ms</a></h4>
<p>text &amp; more</p>
<table><tbody>
<tr><th>Type:</th><td>int</td></tr>
<tr><th>Default:</th><td>300000 (5 minutes)</td></tr>
<tr><th>Valid Values:</th><td>[1,...]</td></tr>
<tr><th>Importance:</th><td>medium</td></tr>
</tbody></table>
</li>
<li>
<h4><a id="errors.tolerance"></a><a id="s_errors.tolerance" href="#x">errors.tolerance</a></h4>
<p>x</p>
<table><tbody>
<tr><th>Type:</th><td>string</td></tr>
<tr><th>Default:</th><td>none</td></tr>
<tr><th>Valid Values:</th><td>[none, all]</td></tr>
<tr><th>Importance:</th><td>medium</td></tr>
<tr><th>Update Mode:</th><td>cluster-wide</td></tr>
</tbody></table>
</li>
</ul>"""

ADOC = """:_mod-docs-content-type: REFERENCE

[id='type-KafkaUser-{context}']
= `KafkaUser` schema reference

[cols="2,2,3a",options="header"]
|====
|Property |Property type |Description
|spec
|xref:type-KafkaUserSpec-{context}[`KafkaUserSpec`]
|The specification.
|====

[id='type-KafkaUserSpec-{context}']
= `KafkaUserSpec` schema reference

Used in: xref:type-KafkaUser-{context}[`KafkaUser`]

[cols="2,2,3a",options="header"]
|====
|Property |Property type |Description
|authentication
|xref:type-TlsAuth-{context}[`TlsAuth`], xref:type-ScramAuth-{context}[`ScramAuth`]
|Mechanism.

* `tls`
* `scram-sha-512`
|acls
|xref:type-Acl-{context}[`Acl`] array
|Rules. Required.
|config
|map
|Free form.
|====

[id='type-TlsAuth-{context}']
= `TlsAuth` schema reference

Used in: xref:type-KafkaUserSpec-{context}[`KafkaUserSpec`]

It must have the value `tls` for the type `TlsAuth`.
[cols="2,2,3a",options="header"]
|====
|Property |Property type |Description
|type
|string
|Must be `tls`.
|validityDays
|integer
|Days.
|====

[id='type-ScramAuth-{context}']
= `ScramAuth` schema reference

Used in: xref:type-KafkaUserSpec-{context}[`KafkaUserSpec`]

It must have the value `scram-sha-512` for the type `ScramAuth`.
[cols="2,2,3a",options="header"]
|====
|Property |Property type |Description
|type
|string
|Must be `scram-sha-512`.
|====

[id='type-Acl-{context}']
= `Acl` schema reference

Used in: xref:type-KafkaUserSpec-{context}[`KafkaUserSpec`]

xref:type-Acl-schema-{context}[Full list]

include::../api/x.adoc[leveloffset=+1]

[id='type-Acl-schema-{context}']
== `Acl` schema properties

[cols="2,2,3a",options="header"]
|====
|Property |Property type |Description
|type
|string (one of [allow, deny])
|The type.
|operations
|string (one or more of [Read, Write]) array
|Ops.
|secretKeyRef
|https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#secretkeyselector-v1-core[SecretKeySelector]
|Ref.
|====
"""

COBRA = """# `argocd app sync` Command Reference

## argocd app sync

Sync an application

### Options

```
  -N, --app-namespace string   Only sync an application in namespace
      --prune                  Allow deleting unexpected resources
```

### Options inherited from parent commands

```
      --server string   Argo CD server address
```
"""


class ConfigHtmlTests(unittest.TestCase):
    def test_parse(self):
        es = bc.parse_config_html(HTML, "consumer", "f.html", "pin")
        self.assertEqual([e["key"] for e in es], ["max.poll.interval.ms", "errors.tolerance"])
        a, b = es
        self.assertEqual((a["scope"], a["type"], a["default"], a["default_value"]), ("consumer", "int", "300000 (5 minutes)", "300000"))
        self.assertIsNone(a["valid_values_list"])
        self.assertEqual(b["valid_values_list"], ["none", "all"])
        self.assertEqual(b["update_mode"], "cluster-wide")
        self.assertEqual((b["source_file"], b["pin"]), ("f.html", "pin"))

    def test_enum_list(self):
        self.assertEqual(bc.enum_list("[a, b]"), ["a", "b"])
        self.assertIsNone(bc.enum_list("[1,...]"))
        self.assertIsNone(bc.enum_list("non-empty string"))


class AppendixTests(unittest.TestCase):
    def setUp(self):
        self.types, self.roots = bc.parse_appendix(ADOC)

    def test_roots(self):
        self.assertEqual(self.roots, ["KafkaUser"])

    def test_flatten(self):
        out = {}
        bc.flatten(self.types, "KafkaUser", "", {"KafkaUser"}, out)
        self.assertIn("spec.authentication.validityDays", out)
        self.assertIn("spec.acls[].type", out)
        self.assertEqual(sorted(out["spec.authentication.type"]["enum"]), ["scram-sha-512", "tls"])
        self.assertEqual(sorted(out["spec.acls[].type"]["enum"]), ["allow", "deny"])
        self.assertEqual(sorted(out["spec.acls[].operations"]["enum"]), ["Read", "Write"])
        self.assertTrue(out["spec.acls"]["required_hint"])
        self.assertTrue(out["spec.config"]["open"])
        self.assertTrue(out["spec.acls[].secretKeyRef"]["open"])
        self.assertIn("external:SecretKeySelector", out["spec.acls[].secretKeyRef"]["type"])
        self.assertNotIn("spec.quotas", out)


class CliTests(unittest.TestCase):
    def test_cobra(self):
        cmd, own, inh = bc.parse_cobra_md(COBRA)
        self.assertEqual(cmd, "argocd app sync")
        self.assertEqual(own, ["--app-namespace", "--prune"])
        self.assertEqual(inh, ["--server"])

    def test_options_in_command(self):
        self.assertEqual(bc.options_in_command(" --describe --topic t | grep --foo"), ["--describe", "--topic"])
        self.assertEqual(bc.options_in_command(" --add-config 'a=b --c' --x"), ["--add-config", "--x"])

    def test_code_lines_joins_continuations(self):
        text = "x\n```\nkafka-topics.sh --create \\\n  --topic t\n```\n"
        self.assertEqual(list(bc.code_lines(text)), ["kafka-topics.sh --create --topic t"])

    def test_pin_and_missing(self):
        with self.assertRaises(bc.MissingSource):
            bc.need("/nonexistent/path/file")


if __name__ == "__main__":
    unittest.main()
