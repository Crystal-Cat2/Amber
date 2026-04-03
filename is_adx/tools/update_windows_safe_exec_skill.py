from pathlib import Path


SKILL_PATH = Path(r"C:\Users\ASUS\.codex\skills\windows-safe-exec\SKILL.md")


def main() -> None:
    text = SKILL_PATH.read_text(encoding="utf-8")

    old_guardrails = """## Chinese HTML/Text Guardrails
- Do not pass Chinese page titles, Chinese paragraphs, or large Chinese text through shell arguments.
- Do not rely on interactive terminal paste for large Chinese text.
- Pass ASCII-only page keys, file paths, and switches through the shell.
- Keep Chinese text inside Python/script constants, templates, or source files.
- After generating HTML/text output, validate the final file, not just the source code.
- Validate all visible Chinese content, not only `<title>`.
"""

    new_guardrails = """## Chinese HTML/Text Guardrails
- Do not pass Chinese page titles, Chinese paragraphs, or large Chinese text through shell arguments.
- Do not rely on interactive terminal paste for large Chinese text.
- Pass ASCII-only page keys, file paths, and switches through the shell.
- Keep Chinese text inside Python/script constants, templates, or source files.
- Treat Chinese writes to files outside the workspace as a high-risk path.
- Do not send large Chinese payloads through PowerShell here-strings, pipes, or raw shell arguments when writing outside the workspace.
- Prefer direct Python file writes for workspace-external targets; if the shell must stay ASCII-only, keep Chinese in Python constants or Unicode escapes.
- After generating HTML/text output, validate the final file, not just the source code.
- Validate all visible Chinese content, not only `<title>`.
"""

    old_defaults = """## Recommended Defaults
- Small text edits: prefer Python-based controlled rewrites.
- Large generated files: write with Python, then validate UTF-8 and visible text.
- PowerShell scripts: run via `powershell -ExecutionPolicy Bypass -File ...`.
- Validation of Chinese strings from shell: use Unicode-escaped arguments, not raw Chinese shell arguments.
"""

    new_defaults = """## Recommended Defaults
- Small text edits: prefer Python-based controlled rewrites.
- Large generated files: write with Python, then validate UTF-8 and visible text.
- Workspace-external files with Chinese content: use controlled Python writes and avoid shell relay.
- PowerShell scripts: run via `powershell -ExecutionPolicy Bypass -File ...`.
- Validation of Chinese strings from shell: use Unicode-escaped arguments, not raw Chinese shell arguments.
"""

    old_checklist = """## Validation Checklist
- File is UTF-8 without BOM.
- File can be decoded as UTF-8.
- Final output does not contain `???`.
- Final output does not contain `�`.
- Required visible text exists in the generated artifact.
"""

    new_checklist = """## Validation Checklist
- File is UTF-8 without BOM.
- File can be decoded as UTF-8.
- Final output does not contain `???`.
- Final output does not contain `�`.
- Final output does not contain unexpected large runs of `?` in Chinese sections.
- For files expected to contain Chinese, sample-check that required Chinese text exists and was not replaced by `?`.
- Required visible text exists in the generated artifact.
"""

    insert_after = new_checklist
    example_section = """## Workspace-External Chinese Write Example
Wrong:

```powershell
@'
<large Chinese text>
'@ | python -
```

Wrong:

```powershell
python -c \"from pathlib import Path; Path('C:/outside-workspace/file.md').write_text('<large Chinese text>', encoding='utf-8')\"
```

Right:

```powershell
python C:/path/to/write_file.py
python C:/Users/ASUS/.codex/skills/windows-safe-exec/scripts/safe_exec.py validate-text-output --path C:/outside-workspace/file.md --must-contain-unicode '\\u4e2d\\u6587' --reject '?'
```

Use the Python script or source file as the container for Chinese text, and keep shell-facing arguments ASCII-only when possible.
"""

    replacements = [
        (old_guardrails, new_guardrails),
        (old_defaults, new_defaults),
        (old_checklist, new_checklist),
    ]

    for old, new in replacements:
        if old not in text:
            raise SystemExit(f"Expected block not found:\\n{old}")
        text = text.replace(old, new, 1)

    marker = "## Standard Commands\n"
    if marker not in text:
        raise SystemExit("Standard Commands section not found.")
    if "## Workspace-External Chinese Write Example\n" not in text:
        text = text.replace(marker, example_section + "\n" + marker, 1)

    SKILL_PATH.write_text(text, encoding="utf-8", newline="\n")
    print(f"Updated {SKILL_PATH}")


if __name__ == "__main__":
    main()
