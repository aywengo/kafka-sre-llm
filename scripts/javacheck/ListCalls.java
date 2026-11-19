// ListCalls: list the resolved method and constructor calls of one Java source file, in evaluation order,
// with the Compiler Tree API (javax.tools + com.sun.source). Used by scripts/eval2_checks_a3_client.py (WP1c T2).
//
// It parses and attributes the source (JavacTask.parse + analyze). It never generates class files and never loads
// or runs anything it compiled. Annotation processing is off (-proc:none).
//
// Usage: java -cp <dir of ListCalls.class> ListCalls <classpath> <file.java>...   (all files are one compilation)
// Output, one JSON object per line on stdout:
//   {"t":"call","seq":N,"file":"Name.java","line":L,"owner":"binary.Name","receiver":"binary.Name","name":"m","params":["T",..],
//    "args":[{"k":"s","v":"text"} | {"k":"n","v":"1"} | {"k":"b","v":"true"} | {"k":"c","v":"binary.Name"} |
//            {"k":"?"}]}
//   {"t":"err","line":L,"msg":"..."}      a compile error (the calls that resolved are still listed)
//   {"t":"unresolved","line":L,"name":"m"}  a call javac could not resolve
// Constructors are calls named "<init>" with the class constructed as owner and receiver.
import com.sun.source.tree.*;
import com.sun.source.util.*;
import javax.lang.model.element.*;
import javax.lang.model.type.*;
import javax.lang.model.util.*;
import javax.tools.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ListCalls {
    static StringBuilder out = new StringBuilder();
    static Trees trees;
    static Elements elements;
    static Types types;
    static CompilationUnitTree unit;
    static int seq = 0;

    static String esc(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c)); else b.append(c);
            }
        }
        return b.append('"').toString();
    }

    static String typeName(TypeMirror t) {
        if (t == null) return "?";
        t = types.erasure(t);
        switch (t.getKind()) {
            case DECLARED: {
                Element e = ((DeclaredType) t).asElement();
                return elements.getBinaryName((TypeElement) e).toString();
            }
            case ARRAY: return typeName(((ArrayType) t).getComponentType()) + "[]";
            default: return t.toString();
        }
    }

    static String describeConst(Object v) {
        if (v instanceof String) return "{\"k\":\"s\",\"v\":" + esc((String) v) + "}";
        if (v instanceof Boolean) return "{\"k\":\"b\",\"v\":" + esc(v.toString()) + "}";
        if (v instanceof Character) return "{\"k\":\"s\",\"v\":" + esc(v.toString()) + "}";
        if (v instanceof Number) return "{\"k\":\"n\",\"v\":" + esc(v.toString()) + "}";
        return "{\"k\":\"?\"}";
    }

    // The constant value of an expression (literal, constant variable, constant concatenation) or null.
    static Object constValue(TreePath p, ExpressionTree e) {
        TreePath ep = new TreePath(p, e);
        switch (e.getKind()) {
            case STRING_LITERAL: case INT_LITERAL: case LONG_LITERAL: case BOOLEAN_LITERAL:
            case CHAR_LITERAL: case DOUBLE_LITERAL: case FLOAT_LITERAL:
                return ((LiteralTree) e).getValue();
            case PARENTHESIZED:
                return constValue(ep, ((ParenthesizedTree) e).getExpression());
            case IDENTIFIER: case MEMBER_SELECT: {
                Element el = trees.getElement(ep);
                if (el instanceof VariableElement) return ((VariableElement) el).getConstantValue();
                return null;
            }
            case PLUS: {
                BinaryTree b = (BinaryTree) e;
                Object l = constValue(ep, b.getLeftOperand()), r = constValue(ep, b.getRightOperand());
                if (l == null || r == null) return null;
                if (l instanceof String || r instanceof String) return String.valueOf(l) + String.valueOf(r);
                if (l instanceof Integer && r instanceof Integer) return (Integer) l + (Integer) r;
                if (l instanceof Long || r instanceof Long) return ((Number) l).longValue() + ((Number) r).longValue();
                return null;
            }
            default: return null;
        }
    }

    static String describeArg(TreePath p, ExpressionTree e) {
        Object v = constValue(p, e);
        if (v != null) return describeConst(v);
        TreePath ep = new TreePath(p, e);
        // Foo.class
        if (e.getKind() == Tree.Kind.MEMBER_SELECT) {
            MemberSelectTree ms = (MemberSelectTree) e;
            if (ms.getIdentifier().contentEquals("class")) {
                TypeMirror t = trees.getTypeMirror(new TreePath(ep, ms.getExpression()));
                if (t != null && t.getKind() == TypeKind.DECLARED) return "{\"k\":\"c\",\"v\":" + esc(typeName(t)) + "}";
            }
        }
        // Foo.class.getName() / getCanonicalName()
        if (e.getKind() == Tree.Kind.METHOD_INVOCATION) {
            MethodInvocationTree mi = (MethodInvocationTree) e;
            ExpressionTree sel = mi.getMethodSelect();
            if (sel.getKind() == Tree.Kind.MEMBER_SELECT && mi.getArguments().isEmpty()) {
                MemberSelectTree ms = (MemberSelectTree) sel;
                String id = ms.getIdentifier().toString();
                if ((id.equals("getName") || id.equals("getCanonicalName")) && ms.getExpression().getKind() == Tree.Kind.MEMBER_SELECT) {
                    String d = describeArg(new TreePath(ep, sel), ms.getExpression());
                    if (d.startsWith("{\"k\":\"c\"")) return d.replace("\"k\":\"c\"", "\"k\":\"s\"");
                }
            }
        }
        return "{\"k\":\"?\"}";
    }

    static long line(Tree t) {
        long pos = trees.getSourcePositions().getStartPosition(unit, t);
        return pos < 0 ? -1 : unit.getLineMap().getLineNumber(pos);
    }

    static void emit(TreePath path, Tree node, ExpressionTree select, List<? extends ExpressionTree> args, boolean ctor) {
        Element el = trees.getElement(ctor ? path : new TreePath(path, select));
        String name = ctor ? "<init>" : (node instanceof MethodInvocationTree ? simpleName(select) : "?");
        if (!(el instanceof ExecutableElement)) {
            out.append("{\"t\":\"unresolved\",\"line\":").append(line(node)).append(",\"name\":").append(esc(name)).append("}\n");
            return;
        }
        ExecutableElement m = (ExecutableElement) el;
        TypeElement owner = (TypeElement) m.getEnclosingElement();
        String ownerName = elements.getBinaryName(owner).toString();
        String receiver = ownerName;
        if (!ctor && select instanceof MemberSelectTree) {
            TypeMirror rt = trees.getTypeMirror(new TreePath(new TreePath(path, select), ((MemberSelectTree) select).getExpression()));
            if (rt != null && (rt.getKind() == TypeKind.DECLARED || rt.getKind() == TypeKind.ARRAY)) receiver = typeName(rt);
        }
        StringBuilder ps = new StringBuilder("[");
        boolean first = true;
        for (VariableElement p : m.getParameters()) {
            if (!first) ps.append(',');
            first = false;
            ps.append(esc(typeName(p.asType())));
        }
        ps.append(']');
        StringBuilder as = new StringBuilder("[");
        first = true;
        for (ExpressionTree a : args) {
            if (!first) as.append(',');
            first = false;
            as.append(describeArg(path, a));
        }
        as.append(']');
        out.append("{\"t\":\"call\",\"seq\":").append(seq++).append(",\"file\":").append(esc(new File(unit.getSourceFile().getName()).getName()))
           .append(",\"line\":").append(line(node))
           .append(",\"owner\":").append(esc(ownerName)).append(",\"receiver\":").append(esc(receiver))
           .append(",\"name\":").append(esc(ctor ? "<init>" : m.getSimpleName().toString()))
           .append(",\"params\":").append(ps).append(",\"args\":").append(as).append("}\n");
    }

    static String simpleName(ExpressionTree select) {
        if (select instanceof MemberSelectTree) return ((MemberSelectTree) select).getIdentifier().toString();
        if (select instanceof IdentifierTree) return ((IdentifierTree) select).getName().toString();
        return "?";
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) { System.err.println("usage: ListCalls <classpath> <file.java>..."); System.exit(2); }
        JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        try (StandardJavaFileManager fm = jc.getStandardFileManager(diags, null, java.nio.charset.StandardCharsets.UTF_8)) {
            File[] files = new File[argv.length - 1];
            for (int i = 1; i < argv.length; i++) files[i - 1] = new File(argv[i]);
            Iterable<? extends JavaFileObject> units = fm.getJavaFileObjects(files);
            List<String> opts = Arrays.asList("-proc:none", "--release", "17", "-nowarn", "-Xlint:none",
                    "-implicit:none", "-sourcepath", "", "-cp", argv[0]);
            JavacTask task = (JavacTask) jc.getTask(null, fm, diags, opts, null, units);
            trees = Trees.instance(task);
            elements = task.getElements();
            types = task.getTypes();
            Iterable<? extends CompilationUnitTree> parsed = task.parse();
            task.analyze();
            for (CompilationUnitTree cu : parsed) {
                unit = cu;
                new TreePathScanner<Void, Void>() {
                    @Override public Void visitMethodInvocation(MethodInvocationTree n, Void v) {
                        TreePath p = getCurrentPath();
                        scan(n.getMethodSelect(), v);     // receiver chain first, then the arguments, then the call
                        scan(n.getArguments(), v);
                        emit(p, n, n.getMethodSelect(), n.getArguments(), false);
                        return null;
                    }
                    @Override public Void visitNewClass(NewClassTree n, Void v) {
                        TreePath p = getCurrentPath();
                        scan(n.getEnclosingExpression(), v);
                        scan(n.getArguments(), v);
                        scan(n.getClassBody(), v);
                        emit(p, n, null, n.getArguments(), true);
                        return null;
                    }
                }.scan(cu, null);
            }
            for (Diagnostic<? extends JavaFileObject> d : diags.getDiagnostics()) {
                if (d.getKind() == Diagnostic.Kind.ERROR)
                    out.append("{\"t\":\"err\",\"line\":").append(d.getLineNumber()).append(",\"msg\":")
                       .append(esc(d.getMessage(Locale.ROOT))).append("}\n");
            }
        }
        System.out.print(out);
    }
}
