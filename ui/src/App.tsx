import { QuerybookPage } from './pages/QuerybookPage';
import { Component, ReactNode } from 'react';

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

class ErrorBoundary extends Component<
  { children: ReactNode },
  ErrorBoundaryState
> {
  constructor(props: { children: ReactNode }) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('Error caught by boundary:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div
          role="alert"
          style={{
            padding: '20px',
            color: '#f48771',
            backgroundColor: '#1e1e1e',
            minHeight: '100vh',
            fontFamily: 'monospace',
          }}
        >
          <h2>Something went wrong:</h2>
          <pre style={{ color: '#cccccc', whiteSpace: 'pre-wrap' }}>
            {this.state.error?.message}
          </pre>
          {this.state.error?.stack && (
            <pre
              style={{
                color: '#858585',
                fontSize: '12px',
                marginTop: '10px',
                maxHeight: '400px',
                overflow: 'auto',
              }}
            >
              {this.state.error.stack}
            </pre>
          )}
          <button
            onClick={() => {
              this.setState({ hasError: false, error: null });
              window.location.reload();
            }}
            style={{
              marginTop: '20px',
              padding: '10px 20px',
              backgroundColor: '#007ACC',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
            }}
          >
            Reload Page
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

function App() {
  console.log('App component rendering...');
  return (
    <ErrorBoundary>
      <QuerybookPage />
    </ErrorBoundary>
  );
}

export default App;
