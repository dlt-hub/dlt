---
slug: second-data-setup
title: The Second Data Warehouse, aka the "disaster recovery" project
image:  https://storage.googleapis.com/dlt-blog-images/second_house.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [data setup, disaster recovery]
---

# The things i've seen

The last 5 years before working on dlt, I spent as a data engineering freelancer.
Before freelancing, I was working for "sexy but poor" startups where building fast and cheap was a religion.

In this time, I had the pleasure of doing many first time setups, and a few "rebuilds" or "second time setups".

In fact, my first freelancing project was a "disaster recovery" one.

A "second time build" or "disaster recovery project" refers to the process of re-designing, re-building, or significantly
overhauling a data warehouse or data infrastructure after the initial setup has failed to meet the organization's needs.

![dipping your toes in disaster](https://storage.googleapis.com/dlt-blog-images/disaster-2.png)

## The first time builds gone wrong

There's usually no need for a second time build, if the first time build works. Rather, a migration might cut it.
A second time build usually happens only if
- the first time build does not work, either now or for the next requirements.
- the first time build cannot be "migrated" or "fixed" due to fundamental flaws.

Let's take some examples from my experiences.
Example 1: A serial talker takes a lead role at a large, growing startup. They speak like management, so management trusts. A few years later
   - half the pipelines are running on Pentaho + windows, the other are python 2, 3 and written by agencies.
   - The data engineering team quit. They had enough.
   - The remaining data engineers do what they want - a custom framework - or they threaten to quit, taking the only knowledge of the pipelines with them.
   - Solution: Re-write all pipelines in python3, replace custom framework with airflow, add tests, github, and other best pratices.

Example 2: A large international manufacturing company needed a data warehouse.
    - Microsoft sold them their tech+ consultants.
    - 2 years later, it's done but doesn't work (query time impossible)
    - Solution: Teach the home DE team to use redshift and migrate.

Example 3: A non technical professional takes a lead data role and uses a tool to do everything.
    - same as above but the person also hired a team of juniors
    - since there was no sudden ragequit, the situation persisted for a few years
    - after they left, the remaining team removed the tool and re-built.

Example 4: A first time data hire introduces a platform-like tool that's sql centric and has no versioning, api, or programmatic control.
    - after writing 30k+ lines of wet sql, scheduling and making them dependent on each other in this UI tool (without lineage), the person can no longer maintain the reports
    - Quits after arguing with management.
    - Solution: Reverse engineer existing reports, account for bugs and unfulfilled requirements, build them from scratch, occasionally searching the mass of sql. Outcome was under 2k lines.

Example 5: A VC company wants to make a tool that reads metrics from business apps like google ads, Stripe.
    - They end up at the largest local agency, who recommends them a single - tenant SaaS MDS for 90k to set up and a pathway from there
    - They agreed and then asked me to review. The agency person was aggressive and queried my knowledge on unrelated things, in an attempt to dismiss my assessment.
    - Turns out the agency was selling "installing 5tran and cleaning the data" for 5k+ per source, and some implementation partners time.
    - I think the VC later hired a non technical freelancer to do the work.

# Who can build a first time setup that scales into the future?

The non-negotiable skills needed are
- Programming. You can use ETL tools for ingestion, but they rarely solve the problem fully (under 20% in my respondent network - these are generally <30 people companies)
- Modelling. Architecture first, sql second, tools third.
- Requirement collection. You should consult your stakeholders on the data available to represent their process, and reach a good result. Usually the stakeholders are not experts and will not be able to give good requirements.

## Who's to blame and what can we do about it?

I believe the blame is quite shared. The common denominators seem to be
- A lack of technical knowledge,
- tools to fill the gap.
- and a warped or dishonest self representation (by vendor or professional)

As for what to do about it:
If you were a hiring manager, ensure that your first data hire has all the skills at their disposal, and make sure they don't just talk the talk but walk the walk. Ask for references or test them.

But you aren't a hiring manager - those folks don't read data blogs.

So here's what you can do
- Ensure all 3 skills are available - they do not need to all be in one person. You could hire a freelance DE to build first, and a technical analyst to fulfil requests and extend the stack.
- Let vendors write about first data hire, and "follow the money" - Check if the advice aligns with their financial incentive. If it does, get a second opinion.
- Choose tooling that scales across different stages of a data stack lifecycle, so the problem doesn't occur.
- Use vendor agnostic components where possible (for example, dlt + sqlmesh + sql glot can create a db-agnostic stack that enables you to switch between dbs)
- Behave better - the temptation to oversell yourself is there, but you could check yourself and look for a position where you can learn. Your professional network could be your biggest help in your career, don't screw them over.
- Use independent freelancers for consulting. They live off reputation, so look for the recommended ones.

## How to do a disaster recovery?

The problem usually originates from the lack of a skill, which downstreams into implementations that don't scale.
However, the solution is often not as simple as adding the skill, because various workarounds were created to bridge that gap, and those workarounds have people working on them.

Simply adding that missing skill to the team to build the missing piece would create a redundancy, which in its resolution would kick out the existing workarounds.
But workarounds are maintained by roles, so the original implementer will usually feel their position threatened;
This can easily escalate to a people conflict which often leads with the workaround maker quitting (or getting fired).

How to manage the emotions?
- Be considerate of people's feelings - you are brought in to replace their work, so make it a cooperative experience where they can be the hero.
- Ask for help when you are not sure about who has the decision over an area.

How to manage the technical side?
- Ensure you have all the skills needed to deliver a data stack on the team.
- If the existing solution produces correct results, use it as requirements for the next - for example, you could write tests that check that business rules are correctly implemented.
- Clarify with stakeholders how much the old solution should be maintained - it will likely free up people to work on the new one.
- Identify team skills that can help towards the new solution and consider them when choosing the technology stack.


## What I wish I knew

Each "disaster recovery" project was more than just a technical reboot; it was a testament to the team's adaptability,
the foresight in planning for scalability, and, importantly, the humility to recognize and rectify mistakes.
"What I Wish I Knew Then" is about the understanding that building a data infrastructure is as much about
building a culture of continuous learning and improvement as it is about the code and systems themselves.


### Want to discuss?

Agencies and freelancers are often the heavy-lifters that are brought in to do such setups.
Is this something you are currently doing?
Tell us about your challenges so we may better support you.

[Join our slack community](https://dlthub.com/community) to take part in the conversation.
