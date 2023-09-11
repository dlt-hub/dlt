import Admonition from "@theme/Admonition";
import CodeBlock from '@theme/CodeBlock';

<Admonition>
    The <span>{props.title}</span> Example is part of the <a href="/docs/examples">DLT Code Examples</a>, a list of comprehensive examples to help you solve real world loading problems with dlt.
    The source code for this example can be found in our repository at: <a href={"https://github.com/dlt-hub/dlt/tree/devel/docs/examples/" + props.slug}>{"https://github.com/dlt-hub/dlt/tree/devel/docs/examples/" + props.slug}.</a>
</Admonition>

## TLDR
<div>{props.intro}</div>

## Setup: Running this example on your machine
<CodeBlock language="sh">
{`# clone the dlt repository
git clone git@github.com:dlt-hub/dlt.git
# go to example directory
cd ./dlt/docs/examples/${props.slug}
# install dlt
pip3 install dlt
# run the example script
python run.py`}
</CodeBlock>
