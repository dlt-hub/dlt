import Admonition from "@theme/Admonition";
import CodeBlock from '@theme/CodeBlock';

<Admonition>
    The source code for this example can be found in our repository at: <a href={"https://github.com/dlt-hub/dlt/tree/devel/docs/examples/" + props.slug}>{"https://github.com/dlt-hub/dlt/tree/devel/docs/examples/" + props.slug}</a>.
</Admonition>

## TLDR
<div>{props.intro}</div>

## Setup: Running this example on your machine
<CodeBlock language="sh">
{`# clone the dlt repository
git clone git@github.com:dlt-hub/dlt.git
# go to example directory
cd ./dlt/docs/examples/${props.slug}
# install dlt with ${props.destination}
pip install "dlt[${props.destination}]"
# run the example script
python ${props.run_file}.py`}
</CodeBlock>
