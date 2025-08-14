# -*- coding: utf-8 -*-
import os
import re
import json
import yaml
import logging
import argparse
import datetime
from typing import Dict, Tuple, Optional

import requests
import arxiv

# =========================
# 基础设置
# =========================
logging.basicConfig(
    format='[%(asctime)s %(levelname)s] %(message)s',
    datefmt='%m/%d/%Y %H:%M:%S',
    level=logging.INFO
)

BASE_PWC_API = "https://arxiv.paperswithcode.com/api/v0/papers/"
GITHUB_SEARCH_API = "https://api.github.com/search/repositories"
ARXIV_ABS = "https://arxiv.org/abs/"
UA = {"User-Agent": "cv-arxiv-daily/1.0 (+https://github.com/yourname)"}

# =========================
# 配置加载
# =========================
def load_config(config_file: str) -> dict:
    """
    读取配置，并把 keywords 下的 filters 列表拼成 arXiv 查询串
    """
    def pretty_filters(**config) -> dict:
        keywords: Dict[str, str] = {}
        QUOTE = '"'
        OR = ' OR '  # 注意加空格
        def parse_filters(filters: list) -> str:
            parts = []
            for f in filters:
                if len(str(f).split()) > 1:
                    parts.append(QUOTE + str(f) + QUOTE)
                else:
                    parts.append(str(f))
            return OR.join(parts)
        for k, v in config['keywords'].items():
            keywords[k] = parse_filters(v['filters'])
        return keywords

    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
        config['kv'] = pretty_filters(**config)
        logging.info(f'config = {config}')
    return config

# =========================
# 小工具
# =========================
def get_authors(authors, first_author=False):
    if first_author:
        return str(authors[0]) if authors else ""
    return ", ".join(str(a) for a in authors)

def sort_papers(papers: dict) -> dict:
    # 按键（通常是 arxiv_id 去版本号）逆序
    output = {}
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for k in keys:
        output[k] = papers[k]
    return output

def get_code_link(qword: str) -> Optional[str]:
    """
    备用：Github 源码搜索（星数优先）
    速率受限，建议配置 GITHUB_TOKEN 环境变量
    """
    params = {"q": qword, "sort": "stars", "order": "desc"}
    headers = {"Accept": "application/vnd.github+json", **UA}
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        r = requests.get(GITHUB_SEARCH_API, params=params, headers=headers, timeout=10)
        if not r.ok:
            logging.warning(f"GitHub search failed: {r.status_code} {r.text[:160]}")
            return None
        data = r.json()
        if data.get("total_count", 0) > 0 and data.get("items"):
            return data["items"][0].get("html_url")
    except requests.RequestException as e:
        logging.error(f"GitHub request error: {e}")
    return None

# =========================
# 核心：抓取每日论文
# =========================
def get_daily_papers(topic: str, query: str = "slam", max_results: int = 2) -> Tuple[dict, dict]:
    """
    返回两份字典：
      content:     {paper_key: markdown_table_row}
      content_web: {paper_key: markdown_list_line}
    """
    content: Dict[str, str] = {}
    content_to_web: Dict[str, str] = {}

    client = arxiv.Client(
        page_size=100,
        delay_seconds=3,   # 尊重 arXiv 频控
        num_retries=3
    )
    search = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate
    )

    for result in client.results(search):
        paper_id = result.get_short_id()         # 如 2108.09112v1
        paper_title = result.title
        paper_url = result.entry_id
        paper_abstract = result.summary.replace("\n", " ")
        paper_authors = get_authors(result.authors)
        paper_first_author = get_authors(result.authors, first_author=True)
        primary_category = result.primary_category
        publish_time = result.published.date() if result.published else None
        update_time = result.updated.date() if result.updated else None
        comments = result.comment

        logging.info(f"Time = {update_time} title = {paper_title} author = {paper_first_author}")

        # 去版本号
        ver_pos = paper_id.find('v')
        paper_key = paper_id if ver_pos == -1 else paper_id[:ver_pos]
        abs_url = ARXIV_ABS + paper_key

        # Papers with Code
        repo_url = None
        pwc_api = BASE_PWC_API + paper_key
        try:
            r = requests.get(pwc_api, headers=UA, timeout=10)
            if r.ok:
                j = r.json()
                if isinstance(j, dict) and j.get("official"):
                    repo_url = j["official"].get("url")
        except requests.RequestException as e:
            logging.error(f"PWC request error: {e} with id: {paper_key}")

        # 组装行（与原脚本兼容的列格式）
        if repo_url:
            content[paper_key] = "|**{}**|**{}**|{} et.al.|[{}]({})|**[link]({})**|\n".format(
                update_time, paper_title, paper_first_author, paper_key, abs_url, repo_url)
            content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({}), Code: **[{}]({})**\n".format(
                update_time, paper_title, paper_first_author, abs_url, abs_url, repo_url, repo_url)
        else:
            content[paper_key] = "|**{}**|**{}**|{} et.al.|[{}]({})|null|\n".format(
                update_time, paper_title, paper_first_author, paper_key, abs_url)
            content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({})\n".format(
                update_time, paper_title, paper_first_author, abs_url, abs_url)

    return {topic: content}, {topic: content_to_web}

# =========================
# 每周补链：把 JSON 里 null 的代码链接用 PWC 补齐
# =========================
def _parse_md_row_line(s: str) -> Tuple[str, str, str, str, str]:
    """
    从存储在 JSON 的表格行字符串中解析出：
    (date, title, authors, arxiv_id_without_version, code_col_raw)
    说明：此解析基于我们自己写入的固定格式，若格式更改，需要同步更新。
    """
    # 去首尾空白和换行
    s = s.strip()

    # 用 '|' 分割并去除空列（行首行尾会有空）
    parts = [p for p in s.split('|') if p != ""]
    # 预期列： [**date**] [**title**] [authors] [[id](url)] [code/null]
    if len(parts) < 5:
        raise ValueError(f"bad md row: {s}")

    date = parts[0].strip().strip('*')
    title = parts[1].strip().strip('*')
    authors = parts[2].strip()

    # PDF 列形如 [2108.09112](https://arxiv.org/abs/2108.09112)
    pdf_col = parts[3].strip()
    m = re.match(r'\[([^\]]+)\]\(([^)]+)\)', pdf_col)
    arxiv_id = m.group(1) if m else pdf_col
    arxiv_id = re.sub(r'v\d+$', '', arxiv_id.strip())

    code_col = parts[4].strip()  # 可能是 null 或 **[link](url)**

    return date, title, authors, arxiv_id, code_col

def update_paper_links(filename: str):
    """
    每周补全 JSON 内 'null' 的代码链接（调用 PWC）
    """
    if not os.path.exists(filename):
        logging.info(f"{filename} not found, skip update.")
        return

    with open(filename, "r", encoding="utf-8") as f:
        content = f.read().strip()
        m = {} if not content else json.loads(content)

    json_data = m.copy()

    for keywords, v in list(json_data.items()):
        logging.info(f'keywords = {keywords}')
        for paper_id, md_line in list(v.items()):
            try:
                date, title, authors, arxiv_id, code_col = _parse_md_row_line(str(md_line))
            except Exception as e:
                logging.warning(f"parse error for {paper_id}: {e}")
                continue

            # 已有链接就跳过
            if 'null' not in code_col.lower():
                continue

            # 尝试补链
            repo_url = None
            try:
                code_api = BASE_PWC_API + arxiv_id
                r = requests.get(code_api, headers=UA, timeout=10)
                if r.ok:
                    j = r.json()
                    if isinstance(j, dict) and j.get("official"):
                        repo_url = j["official"].get("url")
            except requests.RequestException as e:
                logging.error(f"PWC request error: {e} with id: {arxiv_id}")

            if repo_url:
                new_md = str(md_line).replace('|null|', f'|**[link]({repo_url})**|')
                logging.info(f'补链成功 ID={paper_id}, url={repo_url}')
                json_data[keywords][paper_id] = new_md

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False)

# =========================
# JSON 合并/写入
# =========================
def update_json_file(filename: str, data_dict):
    """
    将 get_daily_papers 返回的多份数据合并进总 JSON
    """
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read().strip()
            m = {} if not content else json.loads(content)
    else:
        m = {}

    json_data = m.copy()

    for data in data_dict:
        for keyword in data.keys():
            papers = data[keyword]
            if keyword in json_data:
                json_data[keyword].update(papers)
            else:
                json_data[keyword] = papers

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False)

# =========================
# JSON -> Markdown
# =========================
def json_to_md(filename: str, md_filename: str,
               task: str = '',
               to_web: bool = False,
               use_title: bool = True,
               use_tc: bool = True,
               show_badge: bool = True,
               use_b2t: bool = True):
    """
    将 JSON 渲染为 Markdown
    """
    def pretty_math_all(s: str) -> str:
        # 简单处理多处 $...$，确保左右加空格（不改变内容）
        def repl(m):
            inner = m.group(0)[1:-1].strip()
            return f' ${inner}$ '
        return re.sub(r'\$[^$]+\$', repl, s)

    DateNow = str(datetime.date.today()).replace('-', '.')

    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read().strip()
            data = {} if not content else json.loads(content)
    else:
        data = {}

    # 清空/创建
    with open(md_filename, "w", encoding="utf-8") as f:
        pass

    with open(md_filename, "a", encoding="utf-8") as f:
        if (use_title and to_web):
            f.write("---\nlayout: default\n---\n\n")

        if show_badge:
            f.write(f"[![Contributors][contributors-shield]][contributors-url]\n")
            f.write(f"[![Forks][forks-shield]][forks-url]\n")
            f.write(f"[![Stargazers][stars-shield]][stars-url]\n")
            f.write(f"[![Issues][issues-shield]][issues-url]\n\n")

        if use_title:
            f.write("## Updated on " + DateNow + "\n")
        else:
            f.write("> Updated on " + DateNow + "\n")

        f.write("> This page is forked from [here](https://github.com/liutaocode/TTS-arxiv-daily)\n\n")

        # 目录
        if use_tc:
            f.write("<details>\n  <summary>Table of Contents</summary>\n  <ol>\n")
            for keyword, day_content in data.items():
                if not day_content:
                    continue
                kw = keyword.replace(' ', '-')
                f.write(f"    <li><a href=#{kw.lower()}>{keyword}</a></li>\n")
            f.write("  </ol>\n</details>\n\n")

        # 正文
        for keyword, day_content in data.items():
            if not day_content:
                continue
            f.write(f"## {keyword}\n\n")

            if use_title:
                if not to_web:
                    f.write("|Publish Date|Title|Authors|PDF|Code|\n|---|---|---|---|---|\n")
                else:
                    f.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    f.write("|:---------|:-----------------------|:---------|:------|:------|\n")

            day_content = sort_papers(day_content)
            for _, v in day_content.items():
                if v is not None:
                    f.write(pretty_math_all(v))

            f.write("\n")
            if use_b2t:
                top_info = f"#Updated on {DateNow}".replace(' ', '-').replace('.', '')
                f.write(f"<p align=right>(<a href={top_info.lower()}>back to top</a>)</p>\n\n")

        if show_badge:
            f.write((
                "[contributors-shield]: https://img.shields.io/github/"
                "contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"
            ))
            f.write((
                "[contributors-url]: https://github.com/Vincentqyw/"
                "cv-arxiv-daily/graphs/contributors\n"
            ))
            f.write((
                "[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/"
                "cv-arxiv-daily.svg?style=for-the-badge\n"
            ))
            f.write((
                "[forks-url]: https://github.com/Vincentqyw/"
                "cv-arxiv-daily/network/members\n"
            ))
            f.write((
                "[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/"
                "cv-arxiv-daily.svg?style=for-the-badge\n"
            ))
            f.write((
                "[stars-url]: https://github.com/Vincentqyw/"
                "cv-arxiv-daily/stargazers\n"
            ))
            f.write((
                "[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/"
                "cv-arxiv-daily.svg?style=for-the-badge\n"
            ))
            f.write((
                "[issues-url]: https://github.com/Vincentqyw/"
                "cv-arxiv-daily/issues\n\n"
            ))

    logging.info(f"{task} finished")

# =========================
# 顶层流程
# =========================
def demo(**config):
    data_collector = []
    data_collector_web = []

    keywords_map = config['kv']
    max_results = config['max_results']
    publish_readme = config['publish_readme']
    publish_gitpage = config['publish_gitpage']
    publish_wechat = config['publish_wechat']
    show_badge = config['show_badge']

    b_update = config['update_paper_links']
    logging.info(f'Update Paper Link = {b_update}')

    if not b_update:
        logging.info("GET daily papers begin")
        for topic, keyword in keywords_map.items():
            logging.info(f"Keyword: {topic}  Query: {keyword}")
            data, data_web = get_daily_papers(topic, query=keyword, max_results=max_results)
            data_collector.append(data)
            data_collector_web.append(data_web)
            print()
        logging.info("GET daily papers end")

    # 1) README
    if publish_readme:
        json_file = config['json_readme_path']
        md_file = config['md_readme_path']
        if b_update:
            update_paper_links(json_file)
        else:
            update_json_file(json_file, data_collector)
        json_to_md(json_file, md_file, task='Update Readme', show_badge=show_badge)

    # 2) GitPage
    if publish_gitpage:
        json_file = config['json_gitpage_path']
        md_file = config['md_gitpage_path']
        if b_update:
            update_paper_links(json_file)
        else:
            update_json_file(json_file, data_collector)
        json_to_md(json_file, md_file, task='Update GitPage',
                   to_web=True, show_badge=show_badge, use_tc=False, use_b2t=False)

    # 3) Wechat
    if publish_wechat:
        json_file = config['json_wechat_path']
        md_file = config['md_wechat_path']
        if b_update:
            update_paper_links(json_file)
        else:
            update_json_file(json_file, data_collector_web)
        json_to_md(json_file, md_file, task='Update Wechat',
                   to_web=False, use_title=False, show_badge=show_badge)

# =========================
# 入口
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path', type=str, default='config.yaml',
                        help='configuration file path')
    parser.add_argument('--update_paper_links', default=False, action="store_true",
                        help='whether to update paper links etc.')
    args = parser.parse_args()

    cfg = load_config(args.config_path)
    cfg = {**cfg, 'update_paper_links': args.update_paper_links}
    demo(**cfg)
