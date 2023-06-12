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
                <h1>Continue Demo</h1>
            </div>
        </div>

        <div className={dhelpModalClassname}>
            <div className='overlayBackground' onClick={() => setDhelpModalOpen(false)}></div>
            <div className='overlayContent'>
                <h1>Dhelp</h1>
            </div>
        </div>

    </div>);
}
