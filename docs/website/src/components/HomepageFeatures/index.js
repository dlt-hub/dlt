import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Easy data loading',
    Svg: require('@site/static/img/loading.svg').default,
    description: (
      <>
        Automatically turn the JSON returned by any API into a live dataset stored wherever you want it.
      </>
    ),
  },
  {
    title: 'Simple python library',
    Svg: require('@site/static/img/python.svg').default,
    description: (
      <>
        <code>pip install python-dlt</code> and then include <code>import dlt</code> to use it in your loading script.
      </>
    ),
  },
  {
    title: 'Powered by open source',
    Svg: require('@site/static/img/open-source.svg').default,
    description: (
      <>
        The <code>dlt</code> library is licensed under the Apache License 2.0, so you can use it for free forever.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
