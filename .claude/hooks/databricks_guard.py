"""PreToolUse hook (Bash) used by the project's subagents.

Modes:
  profile   -> blocks Databricks commands that target a profile other than aws2.
  readonly  -> profile check + only allows read-only Databricks commands
               (SELECT/SHOW/DESCRIBE queries, list/get verbs, fs ls/cat).

Exit 0 allows the call; exit 2 blocks it and the stderr message is shown to the subagent.
Any parsing failure blocks the call (fail closed).
"""

import json
import re
import shlex
import sys

ALLOWED_PROFILE = "aws2"

# Statement types a read-only query may start with.
READ_STATEMENTS = re.compile(r"^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN|VALUES)\b", re.IGNORECASE)
# Keywords that change data, metadata or compute state anywhere in the command.
WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|OPTIMIZE|VACUUM"
    r"|COPY|REFRESH|RESTORE|MSCK|ANALYZE|CACHE|UNCACHE|COMMENT|EXECUTE|CALL)\b",
    re.IGNORECASE,
)
# Query flags that take a value (the value is not SQL).
QUERY_VALUE_FLAGS = {"--warehouse", "--output", "-o", "--param", "--profile", "-p", "--file", "-f"}


def block(reason):
    print(f"Bloqueado pelo databricks_guard: {reason}", file=sys.stderr)
    sys.exit(2)


def tokenize(command):
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    return list(lexer)


def segments(tokens):
    """Splits tokens at shell operators so each simple command is checked on its own."""
    current = []
    for tok in tokens:
        if tok and set(tok) <= set("();<>|&"):
            if current:
                yield current
            current = []
        else:
            current.append(tok)
    if current:
        yield current


def check_profile(command, tokens):
    if re.search(r"DATABRICKS_CONFIG_PROFILE=(?!aws2\b)", command):
        block(f"só o perfil {ALLOWED_PROFILE} é permitido neste projeto.")
    for seg in segments(tokens):
        # Only databricks commands: '-p' means other things elsewhere (mkdir -p).
        if "databricks" not in seg:
            continue
        for i, tok in enumerate(seg):
            value = None
            if tok in ("--profile", "-p") and i + 1 < len(seg):
                value = seg[i + 1]
            elif tok.startswith("--profile="):
                value = tok.split("=", 1)[1]
            if value is not None and value != ALLOWED_PROFILE:
                block(f"perfil '{value}' não permitido; use {ALLOWED_PROFILE}.")


def check_readonly(command, tokens):
    if "$(" in command or "`" in command or "${" in command:
        block("substituição de comando/variável e crases não são permitidas (use read_files() em vez de crases).")
    if any(tok and set(tok) <= set("();<>|&") for tok in tokens):
        block("operadores de shell (; | & > <) fora de aspas não são permitidos: um comando databricks por vez.")
    if not tokens or tokens[0] != "databricks":
        block("este subagente só pode executar comandos 'databricks'.")

    words = [t for t in tokens[1:] if not t.startswith("-")]
    if words and words[0] in ("secrets", "tokens", "token-management", "api", "auth"):
        block(f"o grupo de comandos '{words[0]}' não é permitido (credenciais/API crua).")
    if words[:4] == ["experimental", "aitools", "tools", "query"]:
        check_query(command, tokens)
        return
    if len(words) >= 2 and words[0] == "fs" and words[1] in ("ls", "cat"):
        return
    if len(words) >= 2 and re.match(r"^(list|get)", words[1]):
        return
    block("só são permitidos: query SQL de leitura, verbos list*/get* e fs ls/cat.")


def check_query(command, tokens):
    if WRITE_KEYWORDS.search(command):
        block("a consulta contém palavra-chave de escrita (INSERT, MERGE, DROP, CREATE, ...). Só leitura.")

    start = tokens.index("query") + 1
    statements = []
    i = start
    while i < len(tokens):
        tok = tokens[i]
        if tok in ("--file", "-f") and i + 1 < len(tokens):
            try:
                with open(tokens[i + 1], encoding="utf-8") as fh:
                    statements.append(fh.read())
            except OSError:
                block(f"não consegui ler o arquivo SQL {tokens[i + 1]} para validar.")
            i += 2
        elif tok in QUERY_VALUE_FLAGS:
            i += 2
        elif tok.startswith("-"):
            i += 1
        else:
            statements.append(tok)
            i += 1

    if not statements:
        block("SQL via stdin não é permitido; passe a consulta como argumento.")
    for sql in statements:
        if WRITE_KEYWORDS.search(sql):
            block("o arquivo SQL contém palavra-chave de escrita. Só leitura.")
        if not READ_STATEMENTS.match(sql):
            block("cada consulta deve começar com SELECT, WITH, SHOW, DESCRIBE ou EXPLAIN.")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "readonly"
    try:
        payload = json.load(sys.stdin)
        command = payload.get("tool_input", {}).get("command", "")
        tokens = tokenize(command)
    except Exception as exc:  # fail closed
        block(f"não consegui interpretar o comando ({exc}).")

    check_profile(command, tokens)
    if mode == "readonly":
        check_readonly(command, tokens)
    sys.exit(0)


if __name__ == "__main__":
    main()
