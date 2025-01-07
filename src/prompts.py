SCRAPER_PROMPT_TEMPLATE = """
Extract the relevant data from this page content:

<content>
{markdown_content}
</content>

**Important** FORMAT ALL EXTRACTED FIELD IN AN EASILY READABLE
"""