import React from 'react';
import Heading from '@theme/Heading';

interface IProps {
    title: string;
    slug: string;
}

export const DLTExampleHeader = ({title, slug} : IProps ) => {
    return (<>
        <Heading as="h1">
            Example: {contentTitle}
        </Heading>
    </>)
}; 