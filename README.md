## 🎮 Steam Games Market Analysis Using AWS Cloud & Machine Learning  
**Unlocking insights into 71K+ games to optimize pricing, engagement & distribution strategies** 

## 🌟 Overview  
This project analyzes Steam's $200B+ gaming ecosystem using AWS cloud infrastructure to uncover actionable insights for developers and publishers. By processing 71,716 games and 3.4M+ user recommendations. Insights gathered :

- **Pricing sweet spots** (78% under $10, free games drive 80% more engagement)  
- **Platform dominance** (54K+ Windows-exclusive titles vs 7.8K cross-platform)  
- **ML-powered price predictions** (78.8% accuracy)  

*Why it matters*: Provides data-driven strategies to reduce churn, optimize releases, and maximize revenue in the competitive gaming market.

## 🛠️ Tech Stack  
**Cloud Infrastructure**  
![AWS](https://img.shields.io/badge/AWS-EC2%20|%20S3%20|%20SageMaker%20|%20QuickSight-orange)  
**Data Processing**  
![PySpark](https://img.shields.io/badge/PySpark-3.5.3-red) ![Python](https://img.shields.io/badge/Python-3.10-blue)  
**Machine Learning**  
![SageMaker](https://img.shields.io/badge/AutoML-SageMaker%20Autopilot-yellowgreen)  
**Visualization**  
![QuickSight](https://img.shields.io/badge/Dashboards-AWS%20QuickSight-9cf)  

## 🔧 Technical Approach  
### 1. Data Pipeline Architecture  
- **Ingestion**: 217MB CSV → AWS S3 bucket  
- **Cleaning**:  
  - Imputed 21K+ missing categorical values  
  - Removed $999 VR pricing outlier  
- **Feature Engineering**:  
  - `Game Age` (Release date → years)  
  - `Popularity Tier` (P/N ratio + achievements)
 
### 2. Machine Learning Automation  
**SageMaker Autopilot Configuration**  
| Parameter          | Value         |
|--------------------|---------------|
| Target Variable    | Price         |
| ML Task            | Regression    |
| Best Model         | XGBoost       |
| R² Score           | 0.788         | 

## 3. 📸 Visual Insights  
A dashboard using AWS Quicksight was created and these are the key insights identified:

### 1. Price vs Engagement Correlation  
**Pattern**: Free games showed 80% higher engagement (1,761 vs 980 avg positive ratings) despite lower average playtime (12.7 vs 18.4 hours).

### 2. Release Strategy Optimization  
**Monthly Trends**:  
- March releases: 6,730 games (peak) → 14% lower avg revenue  
- November releases: 5,412 games → 22% higher avg revenue  
*Recommendation*: Avoid Q1 saturation, target holiday releases



## 4. ⚙️ Automation and Monitoring 
**Daily Workflow**  
At **00:00**: Cron-triggered PySpark ETL → SageMaker model retraining → QuickSight dashboard refresh.

*Monitoring*: Email alerts for pipeline failures via AWS SNS.
  
## 📊 Key Results  
### Market Dynamics  
| Metric                | Value         | Insight                         |
|-----------------------|---------------|---------------------------------|
| Avg Game Price        | $7.21         | 78% under $10                   |
| Free Game Engagement  | 1,761 ratings | 80% > Paid games                |
| Windows Exclusives    | 54,997        | 76.7% market share              |


## Files details
Steam-game-analysis using AWS : Project report in pdf

my_manifest : manifest file for connecting S3 bucket to QuickSight in JSON

QuickSight Dashboard : Visualizations dashboard pdf

scripts : Pyspark scripts used throughout the project

S3_load_data.py : Loading dataset from S3.

S3_transform.py : Pre-processing, transformations and aggregations, store back to S3 bucket.

S3_analysis.py : SQL queries executions.

bonus_automation.py : automation script.

bonus_automation_LN.py : final automation script with logging and email notification.
