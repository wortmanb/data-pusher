#!/usr/bin/env python3
"""
Elasticsearch data pusher for high-volume timeseries data.
Pushes random log data at 1000+ documents per second.
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch, helpers
# Removed es_client dependencies (part of curator)
# Removed curator dependency - using direct Elasticsearch connection


class DataPusher:
    def __init__(self, client, index_name, docs_per_second=1000, batch_size=500):
        self.client = client
        self.index_name = index_name
        self.docs_per_second = docs_per_second
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
        
        # Log level templates for realistic log data
        self.log_levels = ['INFO', 'DEBUG', 'WARN', 'ERROR', 'TRACE']
        self.log_level_weights = [40, 30, 15, 10, 5]
        
        # Service names for realistic microservice logs
        self.services = [
            'user-service', 'payment-service', 'inventory-service', 
            'notification-service', 'auth-service', 'catalog-service',
            'order-service', 'shipping-service', 'analytics-service'
        ]
        
        # Sample log messages
        self.log_messages = [
            'Request processed successfully',
            'Database connection established',
            'Cache miss for key {}',
            'Rate limit exceeded for user {}',
            'Payment transaction completed',
            'User authentication successful',
            'API endpoint /api/v1/{} called',
            'Memory usage at {}%',
            'Processing batch of {} items',
            'Connection timeout after {}ms',
            'Scheduled task executed',
            'Configuration reloaded',
            'Healthcheck passed',
            'Queue size: {} items',
            'File upload completed: {} bytes'
        ]
        
        # Numeric field ranges for realistic metrics
        self.metrics_ranges = {
            'response_time_ms': (10, 5000),
            'memory_usage_mb': (100, 4096),
            'cpu_usage_percent': (1, 100),
            'request_count': (1, 1000),
            'error_count': (0, 50),
            'bytes_processed': (1024, 104857600)  # 1KB to 100MB
        }

    def generate_document(self, timestamp=None):
        """Generate a single timeseries document with realistic log data"""
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        # Add some randomness to timestamp (within last 5 minutes)
        timestamp = timestamp - timedelta(seconds=random.randint(0, 300))
        
        service = random.choice(self.services)
        level = random.choices(self.log_levels, weights=self.log_level_weights)[0]
        
        # Generate realistic log message
        message_template = random.choice(self.log_messages)
        if '{}' in message_template:
            # Fill in placeholders with random values
            placeholders = []
            for _ in range(message_template.count('{}')):
                placeholder = random.choice([
                    str(random.randint(1, 1000)),
                    f"user_{random.randint(1000, 9999)}",
                    f"session_{random.randint(10000, 99999)}",
                    f"{random.randint(50, 95)}",
                    f"orders",
                    f"products",
                    f"{random.randint(100, 10000)}"
                ])
                placeholders.append(placeholder)
            message = message_template.format(*placeholders)
        else:
            message = message_template
            
        # Generate metrics
        metrics = {}
        for metric, (min_val, max_val) in self.metrics_ranges.items():
            if random.random() < 0.7:  # 70% chance to include each metric
                if 'percent' in metric or 'count' in metric:
                    metrics[metric] = random.randint(min_val, max_val)
                else:
                    metrics[metric] = round(random.uniform(min_val, max_val), 2)
        
        # Create the document
        doc = {
            '@timestamp': timestamp.isoformat() + 'Z',
            'service': service,
            'level': level,
            'message': message,
            'environment': random.choice(['prod', 'staging', 'dev']),
            'host': f"host-{random.randint(1, 20):02d}",
            'request_id': f"req_{random.randint(100000, 999999)}",
            'user_id': random.randint(1000, 50000) if random.random() < 0.8 else None,
            'session_id': f"sess_{random.randint(1000000, 9999999)}",
            **metrics
        }
        
        # Add error details for ERROR level logs
        if level == 'ERROR':
            doc['error'] = {
                'type': random.choice(['TimeoutException', 'ConnectionError', 'ValidationError', 'AuthError']),
                'stack_trace': f"at {service}.handler.process() line {random.randint(50, 500)}"
            }
            
        return doc

    def generate_batch(self, batch_size, timestamp=None):
        """Generate a batch of documents"""
        return [self.generate_document(timestamp) for _ in range(batch_size)]

    def bulk_index_batch(self, documents):
        """Index a batch of documents using bulk API"""
        actions = []
        for doc in documents:
            action = {
                '_index': self.index_name,
                '_source': doc,
                '_op_type': 'create'  # Required for data streams
            }
            actions.append(action)
        
        try:
            # Use bulk with more detailed error handling
            response = helpers.bulk(
                self.client, 
                actions, 
                chunk_size=self.batch_size,
                raise_on_error=False,
                raise_on_exception=False
            )
            
            success_count = response[0]
            errors = response[1] if len(response) > 1 else []
            
            if errors:
                # Log first few errors for debugging
                for i, error in enumerate(errors[:3]):
                    self.logger.error(f"Document indexing error {i+1}: {error}")
                self.logger.error(f"Bulk indexing failed: {len(errors)} document(s) failed to index.")
            
            return success_count
            
        except Exception as e:
            self.logger.error(f"Bulk indexing exception: {e}")
            return 0

    def push_data(self, duration_seconds=60, num_threads=4, infinite=False):
        """Push data for specified duration at target rate"""
        self.logger.info(f"Starting data push to index '{self.index_name}'")
        if infinite or duration_seconds == 0:
            self.logger.info(f"Target rate: {self.docs_per_second} docs/sec (infinite mode)")
        else:
            self.logger.info(f"Target rate: {self.docs_per_second} docs/sec for {duration_seconds} seconds")
        self.logger.info(f"Using {num_threads} threads with batch size {self.batch_size}")
        
        start_time = time.time()
        end_time = start_time + duration_seconds if not infinite and duration_seconds > 0 else float('inf')
        total_docs = 0
        
        # Calculate timing
        batches_per_second = self.docs_per_second / self.batch_size
        batch_interval = 1.0 / batches_per_second if batches_per_second > 0 else 0.1
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            last_batch_time = start_time
            
            while time.time() < end_time:
                current_time = time.time()
                
                # Submit batches to maintain target rate
                if current_time - last_batch_time >= batch_interval:
                    timestamp = datetime.utcnow()
                    documents = self.generate_batch(self.batch_size, timestamp)
                    
                    future = executor.submit(self.bulk_index_batch, documents)
                    futures.append(future)
                    last_batch_time = current_time
                
                # Collect completed batches
                completed_futures = []
                for future in futures:
                    if future.done():
                        try:
                            docs_indexed = future.result()
                            total_docs += docs_indexed
                        except Exception as e:
                            self.logger.error(f"Batch failed: {e}")
                        completed_futures.append(future)
                
                # Remove completed futures
                for future in completed_futures:
                    futures.remove(future)
                
                # Small sleep to prevent busy waiting
                time.sleep(0.01)
            
            # Wait for remaining batches to complete
            for future in as_completed(futures):
                try:
                    docs_indexed = future.result()
                    total_docs += docs_indexed
                except Exception as e:
                    self.logger.error(f"Final batch failed: {e}")
        
        elapsed_time = time.time() - start_time
        actual_rate = total_docs / elapsed_time if elapsed_time > 0 else 0
        
        self.logger.info(f"Indexing completed!")
        self.logger.info(f"Total documents indexed: {total_docs}")
        self.logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        self.logger.info(f"Actual rate: {actual_rate:.1f} docs/sec")
        
        return total_docs, actual_rate


def setup_logging(verbose=False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_client(config_file=None, host='localhost:9200', username=None, password=None, verify_certs=True):
    """Create Elasticsearch client using direct connection"""
    # Always use direct connection (curator dependency removed)
    if '://' not in host:
        host = f'http://{host}'
    
    # Build connection parameters
    es_params = {
        'hosts': [host],
        'request_timeout': 60,
        'verify_certs': verify_certs
    }
    
    # Add authentication if provided
    if username and password:
        es_params['basic_auth'] = (username, password)
    
    # For HTTPS connections with disabled cert verification, add SSL context
    if host.startswith('https://') and not verify_certs:
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        es_params['ssl_context'] = ssl_context
    
    return Elasticsearch(**es_params)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Push random timeseries data to Elasticsearch at high volume',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python data-pusher.py --index logs-2024 --rate 1500 --duration 120
  python data-pusher.py --index my-datastream --host https://elastic.example.com:9200
  python data-pusher.py --index test-logs --host https://elastic.example.com:9200 --username elastic --password mypassword
  ES_USERNAME=elastic ES_PASSWORD=mypassword python data-pusher.py --index test-logs
        """
    )
    
    parser.add_argument(
        '--index', 
        required=True,
        help='Target index or datastream name'
    )
    parser.add_argument(
        '--rate', 
        type=int, 
        default=1000,
        help='Target documents per second (default: 1000)'
    )
    parser.add_argument(
        '--duration', 
        type=int, 
        default=0,
        help='Duration to run in seconds (default: 0 for infinite)'
    )
    parser.add_argument(
        '--infinite', 
        action='store_true',
        help='Run indefinitely (overrides --duration)'
    )
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=500,
        help='Batch size for bulk indexing (default: 500)'
    )
    parser.add_argument(
        '--threads', 
        type=int, 
        default=4,
        help='Number of worker threads (default: 4)'
    )
    parser.add_argument(
        '--config', 
        default=None,
        help='Configuration file (currently unused - kept for compatibility)'
    )
    parser.add_argument(
        '--host', 
        default='localhost:9200',
        help='Elasticsearch host (default: localhost:9200)'
    )
    parser.add_argument(
        '--username', 
        default=os.getenv('ES_USERNAME'),
        help='Elasticsearch username for authentication (default: ES_USERNAME env var)'
    )
    parser.add_argument(
        '--password', 
        default=os.getenv('ES_PASSWORD'),
        help='Elasticsearch password for authentication (default: ES_PASSWORD env var)'
    )
    parser.add_argument(
        '--no-verify-certs',
        action='store_true',
        help='Disable SSL certificate verification (for self-signed certificates)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    # Config file argument is kept for compatibility but not used
    config_file = args.config
    if config_file:
        logger.warning(f"Config file {config_file} specified but curator dependency removed - using direct connection to {args.host}")
    
    try:
        # Create Elasticsearch client
        verify_certs = not args.no_verify_certs
        client = create_client(None, args.host, args.username, args.password, verify_certs)
        
        # Test connection
        info = client.info()
        logger.info(f"Connected to Elasticsearch {info['version']['number']} at {info['cluster_name']}")
        
        # Create data pusher and start pushing
        pusher = DataPusher(
            client=client,
            index_name=args.index,
            docs_per_second=args.rate,
            batch_size=args.batch_size
        )
        
        # Handle infinite mode (default is infinite with duration=0)
        infinite_mode = args.infinite or args.duration == 0
        
        total_docs, actual_rate = pusher.push_data(
            duration_seconds=args.duration,
            num_threads=args.threads,
            infinite=infinite_mode
        )
        
        # Print summary
        print(f"\n=== Summary ===")
        print(f"Index: {args.index}")
        print(f"Documents indexed: {total_docs:,}")
        print(f"Target rate: {args.rate:,} docs/sec")
        print(f"Actual rate: {actual_rate:.1f} docs/sec")
        print(f"Duration: {args.duration} seconds")
        
        if actual_rate >= args.rate * 0.9:  # Within 10% of target
            print("✓ Target rate achieved!")
        else:
            print("⚠ Target rate not fully achieved - consider adjusting batch size or threads")
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())