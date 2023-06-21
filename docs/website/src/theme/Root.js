import React, {useState, useEffect} from 'react';
import {useLocation} from '@docusaurus/router';

// inject overlay content in root element
export default function Root({children}) {
  return <>{children}<Overlay /></>;
}

// overlay config
const overlays = {
    "/docs/walkthroughs/create-a-pipeline": {
        buttonTitle: "Create a pipeline with GPT-4",
        title: "Create a pipeline with GPT-4",
        loomId: "aa33b4b9f1ac47f99076644315c9b0ff",
        text: "Create dlt pipeline using the data source of your liking and let the GPT-4 write the resource functions and help you to debug the code."
    }
}


function Overlay() {

  const location = useLocation();

  let overlayConfig = null;
  for (const url in overlays) {
    if (location.pathname.includes(url)) {
        overlayConfig = overlays[url];
    }
    }
    useEffect(() => {
        const handleEsc = (event) => {
           if (event.keyCode === 27) {
            setSontinueModelOpen(false);
            setDhelpModalOpen(false);
          }
        };
        window.addEventListener('keydown', handleEsc);
    
        return () => {
          window.removeEventListener('keydown', handleEsc);
        };
      }, []);

  const loomLink = `https://www.loom.com/embed/${overlayConfig?.loomId}`;

  const [continueModelOpen, setSontinueModelOpen] = useState(false);
  const [dhelpModalOpen, setDhelpModalOpen] = useState(false);

  const continueModalClassname = continueModelOpen ? 'overlay' : 'overlayHidden';
  const dhelpModalClassname = dhelpModalOpen ? 'overlay' : 'overlayHidden';

  return (
    <div className='overlays'>
        {overlayConfig && <a href='#' className='overlayButton overlayButtonBottom continueButton' onClick={() => setSontinueModelOpen(true)}>{overlayConfig.buttonTitle}</a>}
        {/*<a href='#' className='overlayButton overlayButtonBottom' onClick={() => setDhelpModalOpen(true)}>Help Chat</a>*/}

        <div className={continueModalClassname}>
            <div className='overlayBackground' onClick={() => setSontinueModelOpen(false)}></div>
            <div className='overlayContent'>
                <h1>{overlayConfig?.title}</h1>
                <div>{overlayConfig?.text}</div>
                <div className='loomContainer'><iframe src={loomLink} frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen className='loomFrame'></iframe></div>
                <div>This demo works on codespaces. Codespaces is a development environment available for free to anyone with a Github account. You'll be asked to fork the demo repository and from there the README guides you with further steps.</div>
                <div>The demo uses the Continue VSCode extension.</div>
                <br />
                <a className="overlayButton" href="https://github.com/codespaces/new/dlt-hub/dlt-llm-code-playground?ref=create-pipeline">Off to codespaces!</a>
            </div>
        </div>

        <div className={dhelpModalClassname}>
            <div className='overlayBackground' onClick={() => setDhelpModalOpen(false)}></div>
            <div className='overlayContent'>
                <h1>DHelp</h1>
                <h2>Ask a question</h2>
                <div>Welcome to "Codex Central", your next-gen help center, driven by OpenAI's GPT-4 model. It's more than just a forum or a FAQ hub – it's a dynamic knowledge base where coders can find AI-assisted solutions to their pressing problems. With GPT-4's powerful comprehension and predictive abilities, Codex Central provides instantaneous issue resolution, insightful debugging, and personalized guidance. Get your code running smoothly with the unparalleled support at Codex Central - coding help reimagined with AI prowess.</div>
            </div>
        </div>

    </div>);
}
