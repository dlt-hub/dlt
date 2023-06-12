import React, {useState} from 'react';

// Default implementation, that you can customize
export default function Root({children}) {
  return <>{children}<Overlay /></>;
}

function Overlay() {

  const [continueModelOpen, setSontinueModelOpen] = useState(false);
  const [dhelpModalOpen, setDhelpModalOpen] = useState(false);

  const continueModalClassname = continueModelOpen ? 'overlay' : 'overlayHidden';
  const dhelpModalClassname = dhelpModalOpen ? 'overlay' : 'overlayHidden';

  return (
    <div className='overlays'>
        <a href='#' className='overlayButton dhelpButton' onClick={() => setDhelpModalOpen(true)}>DHelp</a>
        <a href='#' className='overlayButton' onClick={() => setSontinueModelOpen(true)}>Continue Demo</a>

        <div className={continueModalClassname}>
            <div className='overlayBackground' onClick={() => setSontinueModelOpen(false)}></div>
            <div className='overlayContent'>
                <h1>dltHub GPT-4 assisted playground</h1>
                <h2>create a pipeline</h2>
                <div>Introducing "Code Sphere", a revolutionary playground for coders, powered by OpenAI's GPT-4 model. It's an immersive, interactive space where developers of all skill levels can experiment, create, and learn. With GPT-4's incredible understanding and generation capabilities, Code Sphere offers real-time coding assistance, smart error correction, and insightful suggestions. Navigate through intricate algorithms, build impressive projects, and supercharge your coding journey with this state-of-the-art platform. Code Sphere - where coding meets AI-powered efficiency.</div>
            </div>
        </div>

        <div className={dhelpModalClassname}>
            <div className='overlayBackground' onClick={() => setDhelpModalOpen(false)}></div>
            <div className='overlayContent'>
                <h1>DHelp</h1>
                <h2>Ask a qestion</h2>
                <div>Welcome to "Codex Central", your next-gen help center, driven by OpenAI's GPT-4 model. It's more than just a forum or a FAQ hub – it's a dynamic knowledge base where coders can find AI-assisted solutions to their pressing problems. With GPT-4's powerful comprehension and predictive abilities, Codex Central provides instantaneous issue resolution, insightful debugging, and personalized guidance. Get your code running smoothly with the unparalleled support at Codex Central - coding help reimagined with AI prowess.</div>
            </div>
        </div>

    </div>);
}
