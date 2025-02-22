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
**Platform Distribution**  
![Platforms]([https://via.placeholder.com/600x400.png/0000FF/FFFFFF?text=Windows+54K+Mac+2.1K+Linux+1.7K](https://github.com/Vineetha1422/BigDataAWS---Steam-game-analysis/blob/main/QuickSight%20Dashboard.pdf))  

## 4. ⚙️ Automation and Monitoring 
**Daily Workflow**  
1. **00:00**: Cron-triggered PySpark ETL  
2. **02:00**: SageMaker model retraining  
3. **04:00**: QuickSight dashboard refresh  

*Monitoring*: Email alerts for pipeline failures via AWS SNS.
  
## 📊 Key Results  
### Market Dynamics  
| Metric                | Value         | Insight                         |
|-----------------------|---------------|---------------------------------|
| Avg Game Price        | $7.21         | 78% under $10                   |
| Free Game Engagement  | 1,761 ratings | 80% > Paid games                |
| Windows Exclusives    | 54,997        | 76.7% market share              |

### ML Performance  
![Model Metrics](https://via.placeholder.com/600x300.png/000/fff?text=R²+0.788+MAE+2.59+RMSE+3.42)  
*SageMaker Autopilot outperformed manual models by 12% in accuracy*.  


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
