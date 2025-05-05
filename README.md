#Steam Analytics 

## Overview
This project creates a comprehensive analytics platform for Steam video games, analyzing the relationship between user sentiment and sales performance. The system collects data from Steam APIs, processes it through AWS services, and presents insights through interactive visualizations.

## Features
- Sentiment analysis of Steam game reviews
- Sales and ownership data collection
- Correlation analysis between game sentiment and market performance
- Interactive dashboards for data exploration
- Historical analysis spanning 5 years of data
- Real-time updates for current market trends

## Data Collection
The platform uses two primary data sources:
1. **Game Metadata & Ownership**: Collected via SteamSpy API
2. **User Reviews**: Collected via Steam Reviews API

Both datasets are processed, cleaned, and integrated to provide a complete view of each game's performance and reception.

## AWS Architecture
This project leverages AWS services for a scalable, cloud-based analytics pipeline:

- **Storage**: Amazon S3, DynamoDB, Redshift
- **Processing**: AWS Lambda, Glue, Comprehend
- **Streaming**: Amazon Kinesis
- **Visualization**: Amazon QuickSight

## Key Visualizations
- Sentiment vs. Sales Time Series Analysis
- Game Performance Quadrant Chart (Loved/Profitable Matrix)
- Sentiment Breakdown by Game Attributes
- Sales Impact Analysis Dashboard

## Installation & Setup
1. Clone this repository
2. Install required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure AWS credentials
4. Run data collection scripts:
   ```
   python steam_game_data.py
   python steam_reviews.py
   ```
5. Deploy AWS resources using CloudFormation templates in the `aws` directory

## Data Collection Scripts
- `steam_game_data.py`: Collects game metadata, ownership, and price data
- `steam_reviews.py`: Collects user reviews with sentiment data

## Project Structure
```
├── data/                  # Raw and processed data storage
├── aws/                   # AWS deployment templates
├── scripts/               # Data collection scripts
├── notebooks/             # Jupyter notebooks for analysis
├── dashboard/             # Dashboard implementation
├── requirements.txt       # Project dependencies
└── README.md              # Project documentation
```

## Future Enhancements
- Additional data sources (e.g., Twitch streaming stats)
- Machine learning models for sales prediction
- Integration with other digital storefronts for comparison
- Mobile app for dashboard access

## License
[MIT License](LICENSE)

## Acknowledgments
- Steam and SteamSpy APIs for data access
- AWS for cloud infrastructure
