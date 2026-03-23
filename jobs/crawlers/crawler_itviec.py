import re

from base_crawler import BaseCrawler


class ITViecCrawler(BaseCrawler):
    LISTING_URL = "https://itviec.com/it-jobs"

    def __init__(self):
        super().__init__("itviec")

    def crawl(self):
        html = self.fetch_html(self.LISTING_URL)
        starts = [match.start() for match in re.finditer(r"data-search--pagination-target='jobCard'>", html)]
        jobs = []

        for index, start in enumerate(starts[:40]):
            end = starts[index + 1] if index + 1 < len(starts) else min(len(html), start + 12000)
            block = html[start:end]

            url_match = re.search(r"data-url='([^']+)'", block)
            title_match = re.search(r"data-url='[^']+'>\s*(.*?)\s*</h3>", block, re.S)
            company_match = re.search(r'class="text-rich-grey" href="/companies/[^"]+">([^<]+)</a>', block)
            salary_match = re.search(r"<div class='d-flex align-items-center salary[^>]*>(.*?)</div>", block, re.S)
            posted_match = re.search(r"<span class='small-text text-dark-grey'>\s*Posted\s*(.*?)\s*</span>", block, re.S)
            tag_matches = re.findall(r'href="/it-jobs/[^"]+">([^<]+)</a>', block)

            if not (url_match and title_match and company_match):
                continue

            title = self.strip_tags(title_match.group(1))
            salary = self.strip_tags(salary_match.group(1)) if salary_match else ""
            posted = self.strip_tags(posted_match.group(1)) if posted_match else ""
            tags = [self.strip_tags(tag) for tag in tag_matches[:3]]
            keyword = tags[0].lower() if tags else "it"

            jobs.append(
                {
                    "title": title,
                    "url": url_match.group(1),
                    "source": "itviec",
                    "company": self.strip_tags(company_match.group(1)),
                    "location": "",
                    "salary": salary,
                    "work_type": "",
                    "keyword": keyword,
                    "posted": posted,
                    "tags": tags,
                }
            )

        jobs = self.unique_by_url(jobs)
        print(f"Collected {len(jobs)} ITviec jobs from {self.LISTING_URL}")
        return jobs


if __name__ == "__main__":
    ITViecCrawler().run()
