SCRAPER_PROMPT_TEMPLATE = """
Extract the relevant data from this page content:

<content>
{markdown_content}
</content>

**Important** FORMAT ALL EXTRACTED FIELD IN AN EASILY READABLE
"""

JOB_POSTS_QUERY = """
{
    job_posts[]{
        title
        description
        job_type
        experience_level
        duration
        rate
        proposal_count
        payment_verified
        country
        ratings
        spent
        skills
    }
}
"""