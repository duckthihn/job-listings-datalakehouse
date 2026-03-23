import re

from base_crawler import BaseCrawler


class TopCVCrawler(BaseCrawler):
    LISTING_URL = "https://www.topcv.vn/tim-viec-lam-it"

    def __init__(self):
        super().__init__("topcv")

    def crawl(self):
        html = self.fetch_html(self.LISTING_URL)
        starts = [match.start() for match in re.finditer(r'job-item-search-result', html)]
        jobs = []

        for index, start in enumerate(starts[:40]):
            end = starts[index + 1] if index + 1 < len(starts) else min(len(html), start + 12000)
            block = html[start:end]

            url_match = re.search(r'href="(https://www\.topcv\.vn/viec-lam/[^"]+)"', block)
            title_match = re.search(r'<span[^>]*title="([^"]+)"', block)
            company_match = re.search(r'<span class="company-name"[^>]*title="([^"]+)"', block)
            salary_match = re.search(r'<label class="salary">\s*<span>\s*(.*?)\s*</span>', block, re.S)
            location_match = re.search(r'<label class="address truncate"[^>]*>(.*?)</label>', block, re.S)

            if not (url_match and title_match and company_match):
                continue

            title = self.strip_tags(title_match.group(1))
            salary = self.strip_tags(salary_match.group(1)) if salary_match else ""
            location = self.strip_tags(location_match.group(1)) if location_match else ""
            keyword = next(
                (term for term in ["data engineer", "data analyst", "python", "backend", "ai"] if term in title.lower()),
                "it",
            )

            jobs.append(
                {
                    "title": title,
                    "url": url_match.group(1),
                    "source": "topcv",
                    "company": self.strip_tags(company_match.group(1)),
                    "location": location,
                    "salary": salary,
                    "work_type": "",
                    "keyword": keyword,
                    "posted": "",
                    "tags": [],
                }
            )

        jobs = self.unique_by_url(jobs)
        print(f"Collected {len(jobs)} TopCV jobs from {self.LISTING_URL}")
        return jobs


if __name__ == "__main__":
    TopCVCrawler().run()
