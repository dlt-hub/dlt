import CodeBlock from '@theme/CodeBlock';
import React from 'react';

export const DltCode = ({children}) => {
    console.log(children);

    return (
    <CodeBlock children={children} language="jsx"/>
)}; 