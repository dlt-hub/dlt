---
slug: governance-democracy-mesh
title: "Shift Left Data Democracy: the link between democracy, governance, data contracts and data mesh."
image:  https://storage.googleapis.com/dlt-blog-images/shift-left-democracy.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [data mesh, shift left, data democracy, pythonic]
---


![shift-left-data-democracy](https://storage.googleapis.com/dlt-blog-images/shift-left-democracy.png)


Definitions of how I use the terms:

Data Governance: A system of oversight and guidance over the data, much like a government is a system of oversight and guidance for a country. The opposite of governance is anarchy, chaos, and entropy.
Data Democracy: A type of governance that ensures stakeholders are part of the governance.
Shift left: Assuming data flows from left to right, shift left represents a focus towards the origin.
Data Mesh: A decentralized data management strategy that treats data as a product, with domain-specific teams managing its quality, governance, and lifecycle.

# Shift Left Data Democracy: From Access to Involvement

In the traditional view, data democracy was largely about democratizing access—ensuring that everyone across the organization could easily retrieve and analyze data. This was a crucial step forward, breaking down silos and making information more available than ever before. However, as we've evolved, so too has our understanding of what true data democracy entails.

Shift left data democracy represents a more profound change. It's not just about making data accessible post-factum; it's about involving a broader spectrum of roles in the very processes of data ingestion, processing, and management. This approach extends the principles of democracy to the entire data lifecycle, beginning with data ingestion.

It's a shift from mere consumption to participation, where access is just the beginning.

### Data mesh is the driver

Just as the data mesh concept emerged to address the complexities of managing data in a distributed, domain-oriented environment, we now see a need for technology to evolve in parallel. The goal?
To put data sources directly in the hands of the people who use them. This means giving teams the tools and autonomy to manage and process their data, ensuring governance and quality from the outset and throughout the data's lifecycle.

This shift left approach to data democracy aligns with the idea behind data mesh,
recognizing that effective data management and governance are not centralized activities but
distributed responsibilities. By involving more stakeholders from the very start of the data flows,
we're not just democratizing access; we're democratizing the entire data flows.

## Governance, from a power game, to a team sport

Democratic access is great but as long as the data producers are not providing clean documented data , we don't
have a democracy. Instead what we have is reality-adjusted communism - we all have plentiful access to the same garbage that the big hippo decided to make in a 5-year plan.

So to gain true democracy, we need to be able to govern democratically also the production and ingestion of data.

### Democracy: The good kind of data governance

Embracing this democratic approach means viewing governance not as a set of restrictions imposed from above
but as a set of guidelines co-created and upheld by everyone. This model ensures that governance
supports rather than stifles innovation, aligning with the principles of accessibility, transparency, and collaboration that define data democracy.

### Linking governance and data mesh

The democratic governance model perfectly fits with data mesh architecture, focusing on decentralized data management.
In a data mesh, domain-specific teams manage their data products while aligning with a unified governance framework.
This approach enhances governance, embedding it into daily data operations for agility and efficiency.

Data mesh distributes governance responsibilities, mirroring the idea of shift-left data democracy.
It transforms every participant into stewards of data quality, security, and compliance.

Through this tight fit between the concepts, shift-left data democracy and data mesh together cultivate a robust, adaptive, and inclusive data culture.

### Distinction between data mesh and shift-left data democracy

While both concepts advocate for decentralized governance, they focus on different aspects of the data lifecycle.
Data mesh architecture emphasizes the structural and operational decentralization of data management, granting autonomy
to domain-specific teams. Shift-left data democracy, on the other hand, extends this decentralization to the very
beginning of the data lifecycle, advocating for early involvement and broad stakeholder participation in governance processes.
Together, they represent a comprehensive approach to democratizing both data management and governance,
ensuring that data serves the collective needs and goals of the organization effectively.

## Practicing shift-left data democracy

So how do we do it?

### Ensuring quality at the source

This strategy ensures that data governance is not just a gatekeeper but an enabler of data democracy, where quality and compliance are built-in, not bolted on.

In the paradigm of shift left data democracy, the initial focus is on establishing explicit source schemas,
implementing schema evolution with alerts, and setting up data contracts. Centralising this information for governance, observability and discovery is also an important part.

This proactive approach is crucial for maintaining governed high data quality across a diverse set of sources. By embedding these mechanisms early on, organizations can prevent the cascade of data issues downstream, significantly reducing the time and resources spent on rectifications.

### Metadata for full lineage

To govern data semantically, we would need semantic tags from the source, that would enable us to know how to work with the data.
Semantic tags could designate PII, security policies, business entities or domains, or anything actionable.
For example, PII tags enables automatic lineage documentation and governance, while other tags could enable automatic access policies or automatic data modelling.

### The role of the Data platform engineer

In a shift left data democracy, the data platform engineer emerges as a crucial supporter, providing the organization with the
tooling and infrastructure to manage data effectively. This section would highlight how these engineers enable democratization
by developing and maintaining tools that empower teams across the organization to manage their own data, ensuring governance,
and facilitating quality control from the get-go.

Simplifying complexity by democratizing tooling
The democratization of data doesn't stop with access or participation; it extends into the tools and
technologies used for data management. This segment would explore the necessity for tools that are accessible and easy to use by everyone in the organization but robust enough to perform complex data engineering tasks. The focus would be on balancing user-friendliness with the sophistication needed to support advanced data operations.

### Simplifying Complexity: Democratizing Tooling

Creating data management tools that strike the right balance between accessibility for all organizational members and the complexity
required for sophisticated data engineering tasks is essential in democratizing data handling.

Tools that are intuitive yet powerful democratize the process of data management, allowing individuals across
the organization, regardless of their technical prowess, to contribute meaningfully to the data lifecycle.

This approach not only empowers a broader spectrum of the workforce to engage with data directly but also
enhances the collective intelligence of the organization by leveraging diverse perspectives. By simplifying
the complexity of data tools without compromising on functionality, organizations can ensure that the democratization
of data does not dilute the quality of data engineering practices but rather enriches the data ecosystem with
contributions from a wider array of stakeholders.

### The role of the data platform engineer
Within the shift left data democracy concept, the role of data platform engineers undergoes a significant evolution.
These professionals are pivotal in empowering the democratization of data, providing the essential tooling and
infrastructure that allow teams across the organization to manage their data autonomously. Data platform engineers
become enablers and facilitators, crafting platforms and tools that embed governance and quality controls right
from the start of the data lifecycle. Their work supports the organization by ensuring that data management
practices are not only compliant and secure but also accessible and intuitive for non-specialists. This shift
underlines a transition from centralized control to distributed empowerment, where data platform engineers support
the broader goal of making data accessible, manageable, and governable across the entire spectrum of the organization,
embodying the very essence of shift left data democracy.

## The Future of Data Management

The trends indicate a move towards more distributed, democratized, and participatory models of data governance and management.

With the increase of consumers at different points in the data flows, a shift towards governing the data at the source or during transport is key.

Tools and strategies will likely advance to further support these models, making data processes even more accessible
and integrated into the fabric of organizational operations. The future promises enhancements in automation, artificial
intelligence, and machine learning to refine data quality, governance, and usability.

As these technologies mature, they will serve to bolster the principles of data democracy, enabling even finer granularity in governance,
more sophisticated data contracts, and richer metadata for comprehensive lineage tracking.

This evolution will not only democratize data governance and management further but also transform data into a more potent,
strategic asset that drives innovation and value creation across all sectors of the economy.

### Want to discuss?

[Join our slack community](https://dlthub.com/community) to take part in the conversation.

