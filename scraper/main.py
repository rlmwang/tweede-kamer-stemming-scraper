# A new beginning, let the behaviour be known
import os
import re
import ssl
from pathlib import Path
from collections.abc import Iterator

import polars as pl
import PyPDF2
import requests
import wget
from bs4 import BeautifulSoup
from tqdm import tqdm

RESULTS_ROOT = Path("results")

STEMMINGSUITSLAGEN_URL = (
    "https://www.tweedekamer.nl/kamerstukken/stemmingsuitslagen"
    "?qry=%2A&fld_tk_categorie=Kamerstukken&fld_prl_kamerstuk=Stemmingsuitslagen"
    "&srt=date%3Adesc%3Adate&page={page}"
)
DEBAT_URL = "https://www.tweedekamer.nl/{link}"
MOTIE_URL = "https://www.tweedekamer.nl/{link}"


STEMMING_SCHEMA = {
    "stemming_id": str,
    "stemming_did": str,
    "title": str,
    "date": str,
    "type": str,
}
MOTIE_SCHEMA = {
    "stemming_id": str,
    "motie_id": str,
    "motie_did": str,
    "document_nr": str,
    "date": str,
    "title": str,
    "besluit": str,
    "uitslag": str,
    "voor": int,
    "vereist": int,
    "totaal": int,
}
INDIENERS_SCHEMA = {
    "motie_id": str,
    "name": str,
    "type": str,
}
DETAILS_SCHEMA = {
    "motie_id": str,
    "fractie": str,
    "zetels": str,
    "stem": str,
}


def run(begin_page, end_page=None):
    end_page = max(begin_page or 0, end_page or 0)

    result = None
    for i in tqdm(range(begin_page, end_page + 1)):
        for data in parse_listings_page(url=STEMMINGSUITSLAGEN_URL.format(page=i)):
            if len(data['stemming']) != 1:
                raise ValueError('Multiple votings in one page')
            stemming_id = data['stemming']['stemming_id'].item()

            write_tables(data, stemming_id)

        break  # TODO: remove debugging break


def parse_listings_page(url) -> Iterator[dict[str, pl.DataFrame]]:
    resp = requests.get(url)
    if not resp.ok:
        raise ValueError(f"Page {url} does not respond")

    page = resp.content
    soup = BeautifulSoup(page, "lxml")
    links = [a["href"] for a in soup.select("h4.u-mt-0 > a")]

    if len(links) == 0:
        raise ValueError("No links found")

    result = None
    for link in links:
        print(link)
        yield parse_stemming_page(url=DEBAT_URL.format(link=link.strip("/")))


def parse_stemming_page(url) -> dict[str, pl.DataFrame]:
    data = create_tables()

    resp = requests.get(url)
    if not resp.ok:
        raise ValueError(f"Page {url} does not respond")
    page = resp.content
    soup = BeautifulSoup(page, "lxml")

    # parse voting

    stemming_info = parse_stemming_page_info(url=url, soup=soup)
    data["stemming"] = pl.concat([data["stemming"], pl.DataFrame(stemming_info)])

    # parse individual motions

    cards = soup.select("div.m-card")
    if len(cards) == 0:
        raise ValueError("No motions found")

    for card in cards:
        # Find the main motion link
        link_tag = card.select_one("h3.m-card__title > a")
        if link_tag is None:
            raise ValueError("Cannot find link of motion")
        link = link_tag["href"]
        print("  " + link)

        # Find the paragraph with "Besluit"
        besluit_tag = None
        for p in card.select("p.u-mt-8"):
            if "Besluit" in p.get_text():
                besluit_tag = p.select_one("span.u-font-bold")
                break
        if besluit_tag is None:
            raise ValueError("Cannot find decision of motion")
        besluit = besluit_tag.get_text(strip=True).strip(".")

        motie_data = parse_motie_page(
            url=MOTIE_URL.format(link=link.strip("/")),
            stemming_id=stemming_info["stemming_id"],
            besluit=besluit,
        )

        data = merge_tables(data, motie_data)

    return data


def parse_stemming_page_info(url: str, soup) -> dict:
    # stemming id & did
    match = re.search(r"id=([^&]+)&did=([^&]+)", url)
    if not match:
        raise ValueError("ID and DID missing from motion")
    stemming_id = match.group(1)
    stemming_did = match.group(2)

    # stemming title
    meta_tag = soup.select_one('meta[name="dcterms.title"]')
    if meta_tag is None:
        ValueError("Stemming title is missing")
    stemming_title = meta_tag["content"]

    # stemming type & date
    h2_tags = soup.select("h2")
    if len(h2_tags) != 1:
        raise ValueError("Stemming subtitle is missing or not unique")

    for h2 in h2_tags:
        span = h2.select_one("span.u-font-normal")
        if span is None:
            raise ValueError("Stemming date is missing")
        stemming_date = span.get_text(strip=True)

        # The meeting type is everything in h2 minus the span
        stemming_type = h2.contents[0].strip() if h2.contents else None
        if type is None:
            raise ValueError("Stemming type is missing")

    return {
        "stemming_id": stemming_id,
        "stemming_did": stemming_did,
        "title": stemming_title,
        "date": stemming_date,
        "type": stemming_type,
    }


def parse_motie_page(
    url: str,
    stemming_id: str,
    besluit: str | None,
) -> dict[str, pl.DataFrame]:
    data = create_tables()

    resp = requests.get(url)
    if not resp.ok:
        raise ValueError(f"Page {url} does not respond")
    page = resp.content
    soup = BeautifulSoup(page, "lxml")

    motie_info = {
        "stemming_id": stemming_id,
        **parse_motie_info(url=url, soup=soup),
        "besluit": besluit,
    }

    indieners_info = parse_indieners_info(soup=soup)
    for row in indieners_info:
        row["motie_id"] = motie_info["motie_id"]

    h2 = soup.find("h2", string=lambda t: t and "Stemmingsuitslagen" in t)
    if h2 is not None:
        # motie uitslag text
        h3 = h2.find_next("h3")
        if h3 is None:
            raise ValueError("Uitslag is missing from stemmingsuitslagen")
        motie_info["uitslag"] = h3.get_text(strip=True)

        # motie uitslag counts
        labels = soup.select("div.m-vote-result__label")
        motie_info["voor"] = int(labels[0].find("span").get_text(strip=True).lstrip(": "))
        motie_info["vereist"] = int(labels[1].find("span").get_text(strip=True).split(": ")[1])
        motie_info["totaal"] = int(labels[2].find("span").get_text(strip=True).split(": ")[1])

        # motie uitslag details
        rows = soup.select("#votes-details table.h-table-bordered tbody tr")[1:]
        details_info = [
            {
                "fractie": r.find_all("td")[0].get_text(strip=True),
                "zetels": int(r.find_all("td")[1].get_text(strip=True)),
                "stem": r.find_all("td")[2].get_text(strip=True),
            }
            for r in rows
        ]
        for row in details_info:
            row["motie_id"] = motie_info["motie_id"]
    else:
        for key in ["uitslag", "voor", "vereist", "totaal"]:
            motie_info[key] = None
        details_info = []

    # dump data into tables
    data["motie"] = pl.concat([data["motie"], pl.DataFrame(motie_info, schema=MOTIE_SCHEMA)])
    data["indieners"] = pl.concat(
        [data["indieners"], pl.DataFrame(indieners_info, schema=INDIENERS_SCHEMA)]
    )
    data["details"] = pl.concat(
        [data["details"], pl.DataFrame(details_info, schema=DETAILS_SCHEMA)]
    )

    return data


def parse_motie_info(url: str, soup) -> dict:
    # motie id & did
    match = re.search(r"id=([^&]+)&did=([^&]+)", url)
    if not match:
        raise ValueError("ID and DID missing from motion")
    motie_id = match.group(1)
    motie_did = match.group(2)

    # motie document nr
    doc_nr_spans = soup.select("span.h-visually-hidden")
    all_doc_nrs = [
        span.next_sibling.strip() for span in doc_nr_spans if "Nummer:" in span.get_text()
    ]
    if len(all_doc_nrs) != 1:
        raise ValueError("Document nr missing or not unique")
    motie_document_nr = all_doc_nrs[0]

    # motie date
    date_spans = soup.select("span.h-visually-hidden")
    all_dates = [span.next_sibling.strip() for span in date_spans if "Datum:" in span.get_text()]
    if len(all_dates) != 1:
        raise ValueError("Date missing or not unique")
    motie_date = all_dates[0]

    # motie title
    all_titles = []
    for h1 in soup.select("h1"):
        span = h1.select_one("span.u-text-primary.u-font-normal")
        if span and span.get_text(strip=True).lower().startswith("motie"):
            all_titles.append(h1.contents[-1].strip())
    if len(all_titles) != 1:
        raise ValueError("Title missing or not unique")
    motie_title = all_titles[0]

    return {
        "motie_id": motie_id,
        "motie_did": motie_did,
        "document_nr": motie_document_nr,
        "date": motie_date,
        "title": motie_title,
    }


def parse_indieners_info(soup) -> list[dict]:
    indieners = []
    for li in soup.select("ul.m-list li.m-list__item--variant-member"):
        # indiener type

        type_span = li.select_one("span.u-font-bold")
        if type_span is None:
            raise ValueError("Type of (mede)indiener is missing")
        type_text = type_span.get_text(strip=True)

        # indiener name
        label_span = li.select_one("span.m-list__label")
        if not label_span:
            raise ValueError("Name of (mede)indiener is missing")

        name_link = label_span.select_one("a.h-link-inverse")
        if name_link:
            # Try first <a>
            name_text = name_link.get_text(strip=True)
        else:
            # Take the text after the type span
            texts = [t.strip() for t in label_span.stripped_strings]
            if len(texts) <= 1:
                raise ValueError("Name of (mede)indiener is missing")
            full_name = texts[1]

            # Split off descriptor at first comma
            if "," in full_name:
                *name_parts, _ = [s.strip() for s in full_name.split(",")]
                name_text = ", ".join(part.strip() for part in name_parts)
            else:
                name_text = full_name

        indieners.append({"type": type_text, "name": name_text})

    return indieners


def parse_motie_page_(url):
    # Catching information of the motion and of the persons who drew or supported the motion
    supporter_info_0 = page.find("h2")
    general_info = page.find("div", class_="col-md-3").find_all("div", class_="link-list__text")
    date = general_info[0].text
    doc_number = general_info[1].text

    # if the motion, identified by the doc_number, is already in the table, then nothing is appended and the function is ended.
    if doc_number in motie_table.document_nr.values:
        return motie_table, indieners_table, stemming_table, activities_table

    state_doc = general_info[2].text
    subject = page.find("h1", class_="section__title").text
    subject = re.sub(" +", " ", subject.replace("\n", ""))
    page_title = page.title.text
    while supporter_info_0.next_sibling.next_sibling is not None:
        supporter_info_0 = supporter_info_0.next_sibling.next_sibling
        raw_individual_info = supporter_info_0.find_all(text=True)
        indieners_table = indieners_table.append(
            {
                "document_nr": doc_number,
                "name_submitter": raw_individual_info[5],
                "submitter_type": raw_individual_info[4],
                "party_submitter": raw_individual_info[7],
                "personal_page": "https://www.tweedekamer.nl{}".format(
                    supporter_info_0.find("a")["href"]
                ),
            },
            ignore_index=True,
        )
    # Catching the Vote (if the vote has been casted)
    if page.find("table", class_="vote-result-table") is None:
        vote_list = "De stemming is niet bekend."
    else:
        tables = page.find_all("table", class_="vote-result-table")
        for table in tables:
            choice = table.th.text
            parties = table.find_all("tr")
            for party in parties[1::]:
                party_name = party.select("td")[0].text
                count_vote = 0
                if len(party.select("td")) > 1:
                    count_vote = int(party.select("td > span")[1].text)
                stemming_table = stemming_table.append(
                    {
                        "document_nr": doc_number,
                        "party_name": party_name.replace("\n", ""),
                        "vote_count": count_vote,
                        "vote": choice,
                    },
                    ignore_index=True,
                )

    # Reading the motion from the PDF. PDF is temporarily downloaded and only the text of the motion is scraped
    sub_url_pdf = page("a", class_="button ___rounded ___download")[0]["href"]
    if sub_url_pdf[-3::] == "pdf":
        pdf_url = "https://www.tweedekamer.nl/" + sub_url_pdf
        ssl._create_default_https_context = (
            ssl._create_unverified_context
        )  # included because of an SSL error on my machine
        reader = PyPDF2.PdfFileReader(wget.download(pdf_url, "downloaded_motie.pdf"))
        pdf_text = reader.getPage(0).extractText()
        t_begin = pdf_text.find("De Kamer")
        ending_note = "en gaat over tot de orde van de dag."
        t_end = pdf_text.find(ending_note)
        motion_text = pdf_text[t_begin:t_end] + ending_note
        motion_text = motion_text.replace("\n", "")
        os.remove("downloaded_motie.pdf")
    else:
        motion_text = "Het document is geen PDF-formaat"

    # Catching de voting and debate activities that are linked to this debate
    if page.find("h2", string="Activiteiten"):
        cards = page.find("h2", string="Activiteiten").parent.find_all("a", class_="card ___small")
        if len(cards) > 0:
            for x in cards:
                activities_url = "https://www.tweedekamer.nl{}".format(x["href"])
                activities_table = activities_table.append(
                    {"document_nr": doc_number, "activities": activities_url}, ignore_index=True
                )

    motie_table = motie_table.append(
        {
            "document_nr": doc_number,
            "Subject": subject,
            "Date": date,
            "Text": motion_text,
            "Title": page_title,
            "State_Document": state_doc,
        },
        ignore_index=True,
    )
    return motie_table, indieners_table, stemming_table, activities_table


# UTILS


def create_tables() -> dict[str, pl.DataFrame]:
    return {
        "stemming": pl.DataFrame(schema=STEMMING_SCHEMA),
        "motie": pl.DataFrame(schema=MOTIE_SCHEMA),
        "indieners": pl.DataFrame(schema=INDIENERS_SCHEMA),
        "details": pl.DataFrame(schema=DETAILS_SCHEMA),
    }


def write_tables(data: dict[str, pl.DataFrame], name: str):
    (RESULTS_ROOT / name).mkdir(parents=True, exist_ok=True)
    for key, table in data.items():
        table.write_csv(RESULTS_ROOT / name / f"{key}.csv")


def merge_tables(
    data_a: dict[str, pl.DataFrame] | None,
    data_b: dict[str, pl.DataFrame],
) -> dict[str, pl.DataFrame]:
    if data_a is None:
        return data_b
    output = {}
    for key in data_a.keys():
        output[key] = pl.concat([data_a[key], data_b[key]])
    return output


if __name__ == "__main__":
    run(2)
