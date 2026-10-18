import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons} style={{display: 'flex', gap: '1rem', justifyContent: 'center', flexWrap: 'wrap', marginTop: '1.5rem'}}>
          <Link
            className="button button--secondary button--lg"
            to="/users/">
            👤 User Guide
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/technical-users/">
            🛠️ Technical User Guide
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/developers/">
            💻 Developer Guide
          </Link>
        </div>
      </div>
    </header>
  );
}

function AudienceCards() {
  return (
    <section style={{padding: '3rem 0'}}>
      <div className="container">
        <div className="row">
          <div className="col col--4 margin-bottom--lg">
            <div className="card shadow--md" style={{height: '100%', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', padding: '1.5rem'}}>
              <div>
                <Heading as="h3">👤 For General Users</Heading>
                <p>
                  Understand how TGDCFS works in everyday plain English. Learn how to connect your favorite WebDAV and SFTP apps like WinSCP, Cyberduck, rclone, or mobile clients to stream media and organize files.
                </p>
              </div>
              <div style={{marginTop: '1rem'}}>
                <Link className="button button--primary button--block" to="/users/">
                  Read User Guide →
                </Link>
              </div>
            </div>
          </div>

          <div className="col col--4 margin-bottom--lg">
            <div className="card shadow--md" style={{height: '100%', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', padding: '1.5rem'}}>
              <div>
                <Heading as="h3">🛠️ For Technical Users</Heading>
                <p>
                  Learn how to deploy TGDCFS using Docker, Docker Compose, or systemd. Master `config.yaml`, client-side AES-256-GCM encryption, RAID-1 store mirroring, local caching, and failover runbooks.
                </p>
              </div>
              <div style={{marginTop: '1rem'}}>
                <Link className="button button--primary button--block" to="/technical-users/">
                  Read Technical Guide →
                </Link>
              </div>
            </div>
          </div>

          <div className="col col--4 margin-bottom--lg">
            <div className="card shadow--md" style={{height: '100%', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', padding: '1.5rem'}}>
              <div>
                <Heading as="h3">💻 For Developers</Heading>
                <p>
                  Step-by-step instructions for setting up the Python 3.13+ repository locally, running test suites with pytest, static typechecking with mypy, linting with ruff, and submitting contributions.
                </p>
              </div>
              <div style={{marginTop: '1rem'}}>
                <Link className="button button--primary button--block" to="/developers/">
                  Read Developer Guide →
                </Link>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} - Documentation`}
      description="Clear, accessible documentation for TGDCFS (Telegram & Discord as storage backends)">
      <HomepageHeader />
      <main>
        <AudienceCards />
      </main>
    </Layout>
  );
}
