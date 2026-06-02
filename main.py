import asyncio
from datetime import datetime

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

from config import config
from crawler import CNInfoHedgeCrawler
from util import is_date_string


@register("cninfo_hedge", "interstellar", "巨潮资讯套保公告查询", "1.1.0")
class CNInfoHedgePlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.plugin_config = config or {}
        self._crawler: CNInfoHedgeCrawler | None = None
        logger.info("巨潮资讯套期保值公告插件已加载")

    async def initialize(self) -> None:
        try:
            from astrbot.api.star import StarTools
            data_dir = StarTools.get_data_dir("cninfo_hedge")
            logs_dir = data_dir / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            config.set_data_dir(data_dir)
            config.set_logs_dir(logs_dir)
            logger.info(f"插件数据目录：{data_dir}")
        except Exception as e:
            logger.warning(f"获取插件数据目录失败：{e}，使用默认目录")

    async def terminate(self) -> None:
        if self._crawler is not None:
            self._crawler.shutdown()
            self._crawler = None
            logger.info("爬虫资源已释放")

    def _get_crawler(self) -> CNInfoHedgeCrawler:
        if self._crawler is None:
            self._crawler = CNInfoHedgeCrawler()
            self._crawler.initialize()
        return self._crawler

    @filter.command("套保查询")
    @filter.command("套保")
    async def search_handler(self, event: AstrMessageEvent, message: str = ""):
        args = message.strip().split() if message and message.strip() else []
        keyword = config.DEFAULT_KEYWORD
        start_date = None
        end_date = None
        max_pages = 1

        for arg in args:
            if is_date_string(arg):
                if start_date is None:
                    start_date = arg
                elif end_date is None:
                    end_date = arg
            elif arg.isdigit():
                max_pages = int(arg)
            else:
                keyword = arg

        yield event.plain_result(f"正在查询套期保值公告，关键词：{keyword}...")

        try:
            crawler = self._get_crawler()
            result = await asyncio.to_thread(
                crawler.search_announcements_sync,
                keyword=keyword,
                start_date=start_date,
                end_date=end_date,
                max_pages=max_pages,
            )
            if result.get("success"):
                total = result.get("total", 0)
                announcements = result.get("announcements", [])[:5]
                reply = f"找到 {total} 条套期保值公告：\n\n"
                for i, ann in enumerate(announcements, 1):
                    date_str = ""
                    if ann.get("publishTime"):
                        try:
                            ts = int(ann["publishTime"]) / 1000
                            date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                        except (ValueError, TypeError):
                            date_str = str(ann["publishTime"])[:10]
                    reply += f"{i}. {ann.get('secName', '')}({ann.get('secCode', '')})\n"
                    reply += f"   标题：{ann.get('title', '')}\n"
                    reply += f"   日期：{date_str}\n"
                    reply += f"   链接：{ann.get('pdfUrl', '')}\n\n"
                if total > 5:
                    reply += f"... 还有 {total - 5} 条，请缩小日期范围\n"
                yield event.plain_result(reply)
                return
            yield event.plain_result(f"查询失败：{result.get('error', '未知错误')}")
        except Exception as e:
            logger.error(f"查询出错：{e}")
            yield event.plain_result(f"查询出错：{str(e)}")
