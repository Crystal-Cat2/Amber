"""源头追溯: HTML 文件 → 生成脚本 + CSV 数据文件"""
import os
import re
import glob
from urllib.parse import unquote, urlparse

from config import AMBER_ROOT


def _file_url_to_path(url: str) -> str | None:
    """file:// URL → 本地路径"""
    if not url.startswith("file:///"):
        return None
    path = unquote(url[8:])  # file:///D:/... → D:/...
    # 处理 Windows 路径
    path = path.replace("/", os.sep)
    if os.path.exists(path):
        return path
    # 尝试正斜杠版本
    path = unquote(url[8:])
    if os.path.exists(path):
        return path
    return None


def _find_project_root(html_path: str) -> str | None:
    """从 HTML 路径向上找项目根目录（含 scripts/ 子目录的目录）"""
    d = os.path.dirname(html_path)
    for _ in range(5):  # 最多向上 5 层
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
        if os.path.isdir(os.path.join(d, "scripts")):
            return d
    return None


def _extract_csv_files_from_script(script_path: str) -> list[str]:
    """从 Python 脚本中提取引用的 CSV 文件名"""
    csv_files = []
    try:
        with open(script_path, encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return csv_files

    # 匹配 read_csv('xxx.csv') 或 read_csv("xxx.csv")
    for m in re.finditer(r"""read_csv\s*\(\s*['"]([^'"]+\.csv)['"]""", content):
        csv_files.append(m.group(1))

    # 匹配 DATA_DIR / "xxx.csv" 或 Path(...) / "xxx.csv"
    for m in re.finditer(r"""(?:DATA_DIR|data_dir|DATA)\s*/\s*['"]([^'"]+\.csv)['"]""", content, re.IGNORECASE):
        if m.group(1) not in csv_files:
            csv_files.append(m.group(1))

    # 匹配 DEFAULT_*_CSV = ... / "xxx.csv"
    for m in re.finditer(r"""DEFAULT_\w+_CSV\s*=\s*\w+\s*/\s*['"]([^'"]+\.csv)['"]""", content):
        if m.group(1) not in csv_files:
            csv_files.append(m.group(1))

    return csv_files


def _find_script_for_html(html_path: str, project_root: str) -> str | None:
    """启发式匹配: HTML 文件名 → 生成脚本"""
    html_name = os.path.splitext(os.path.basename(html_path))[0]  # ad_kill_dashboard
    scripts_dir = os.path.join(project_root, "scripts")

    if not os.path.isdir(scripts_dir):
        return None

    # 策略1: 精确匹配 build_{name}.py 或 gen_{name}.py
    for prefix in ("build_", "gen_", "run_"):
        candidate = os.path.join(scripts_dir, f"{prefix}{html_name}.py")
        if os.path.exists(candidate):
            return candidate

    # 策略2: 脚本中包含输出文件名
    html_basename = os.path.basename(html_path)
    for py_file in glob.glob(os.path.join(scripts_dir, "*.py")):
        try:
            with open(py_file, encoding="utf-8") as f:
                content = f.read()
            if html_basename in content:
                return py_file
        except Exception:
            continue

    # 策略3: 文件名部分匹配（去掉常见前缀后比较）
    for py_file in glob.glob(os.path.join(scripts_dir, "*.py")):
        py_name = os.path.splitext(os.path.basename(py_file))[0]
        # 去掉 build_/gen_/run_ 前缀
        for prefix in ("build_", "gen_", "run_"):
            if py_name.startswith(prefix):
                py_name = py_name[len(prefix):]
                break
        # 检查是否有显著重叠
        if html_name in py_name or py_name in html_name:
            return py_file

    return None


def _detect_run_command(script_path: str) -> list[str]:
    """检测脚本的运行命令（是否支持 --skip-query 等参数）"""
    cmd = ["python", script_path]
    try:
        with open(script_path, encoding="utf-8") as f:
            content = f.read()
        if "--skip-query" in content:
            cmd.append("--skip-query")
    except Exception:
        pass
    return cmd


def resolve_source(page_url: str, page_meta: dict | None = None) -> dict | None:
    """
    解析页面来源信息。

    返回:
        {
            "script_path": str,       # 生成脚本绝对路径
            "csv_files": [str, ...],  # CSV 文件绝对路径列表
            "data_dir": str,          # 数据目录
            "output_html": str,       # HTML 输出路径
            "run_command": [str, ...], # 重新生成命令
            "project_root": str,      # 项目根目录
        }
    或 None（无法追溯）
    """
    page_meta = page_meta or {}

    # 1. 尝试从 meta 标签获取
    if page_meta.get("source_script"):
        script_rel = page_meta["source_script"]
        script_path = os.path.normpath(os.path.join(AMBER_ROOT, script_rel))
        if os.path.exists(script_path):
            data_rel = page_meta.get("source_data", "")
            data_dir = os.path.normpath(os.path.join(AMBER_ROOT, data_rel)) if data_rel else None
            csv_names = _extract_csv_files_from_script(script_path)
            csv_files = []
            if data_dir:
                csv_files = [os.path.join(data_dir, c) for c in csv_names]
            html_path = _file_url_to_path(page_url)
            return {
                "script_path": script_path,
                "csv_files": [c for c in csv_files if os.path.exists(c)],
                "data_dir": data_dir,
                "output_html": html_path,
                "run_command": _detect_run_command(script_path),
                "project_root": os.path.dirname(os.path.dirname(script_path)),
            }

    # 2. 启发式: 从 file:// URL 追溯
    html_path = _file_url_to_path(page_url)
    if not html_path:
        return None

    project_root = _find_project_root(html_path)
    if not project_root:
        return None

    script_path = _find_script_for_html(html_path, project_root)
    if not script_path:
        return None

    csv_names = _extract_csv_files_from_script(script_path)
    data_dir = os.path.join(project_root, "data")
    csv_files = [os.path.join(data_dir, c) for c in csv_names if os.path.exists(os.path.join(data_dir, c))]

    return {
        "script_path": script_path,
        "csv_files": csv_files,
        "data_dir": data_dir if os.path.isdir(data_dir) else None,
        "output_html": html_path,
        "run_command": _detect_run_command(script_path),
        "project_root": project_root,
    }
