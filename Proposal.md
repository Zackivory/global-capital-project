   
# Project Proposal: Predicting Stock Market Performance Using Financial News

## 1. Summary

This project aims to develop a model that leverages daily financial news to predict next-day stock opening price percentage change in the Shanghai Stock Exchange (SSE) and Shenzhen Stock Exchange (SZSE). By analyzing historical financial news from Chinese mainstream media over the past five years and corresponding stock data, the model will assess the influence of policy announcements and other news on short-term market movements.

## 2. Background and Context

Recent policy stimuli in China, along with major global financial events such as the Federal Reserve cutting interest rates, have led to significant fluctuations in the stock market, affecting individual portfolios, including my own. With a background in economics and active involvement in trading on the SSE and SZSE, I am keen to explore the interplay between policy announcements and stock performance. Understanding this relationship is crucial for making informed investment decisions and could contribute valuable insights into the market dynamics influenced by governmental policies.


## 3. Mathematical Formulation

To predict the next-day stock performance, we will use a model that incorporates the following mathematical components:

1. **News Embedding**:let $`N={N_{t_1},N_{t_2},N_{t_3}...N_{t_n}}`$ be the set of all financial news from date $`t_1`$ to $`t_n`$, where $`N_t={n_{t,1},n_{t,2},n_{t,3}...n_{t,m}}`$ represent the set of financial news that published after 3 pm of date $`t`$ and 9 am before date $`t+1`$. We will convert each news $`n_{t,m}`$ in $`N_t`$ into a numerical vector $`E_{n_{t,m}}`$ using OpenAI's Embedding API.

$`E_{n_{t,m}} = \text{Embedding}(n_{t,m})`$

2. **Similarity Search**: For each current news $`n_{t,m} \text{ in } N_t`$, we will find the three most similar historical news $`h_m\prime,h_m\prime\prime,h_m\prime\prime\prime \text{ in } N`$ by computing similarity score with all historical news in $`N`$ and use the most similar three historical news. The similarity score $`S`$ can be computed using cosine similarity:

$`S(E_{n_{t,m}}, E_{n_{t-i,j}}) = \frac{E_{n_{t,m}} \cdot E_{n_{t-i,j}}}{\|E_{n_{t,m}}\| \|E_{n_{t-i,j}}\|}`$
#### for $`h_m\prime`$ which is the most similar historical news to $`n_{t,m}`$
$`S_m\prime`$: denote the similarity score between $`n_{t,m}`$ and $`h_m\prime`$,

$`t_m\prime`$: denote the next trading day of the historical news $`h_m\prime`$ 

$`P_{t_m}\prime `$: denote the Backward Adjusted openig price percentage change on date $`t_m\prime`$ 

#### for $`h_m\prime\prime`$ which is the second most similar historical news to $`n_{t,m}`$
$`S_m\prime\prime`$: denote the similarity score between $`n_{t,m}`$ and $`h_m\prime\prime`$,

$`t_m\prime\prime`$: denote the next trading day of the historical news $`h_m\prime\prime`$ 

$`P_{t_m}\prime\prime `$: denote the Backward Adjusted openig price percentage change on date $`t_m\prime\prime`$ 

#### for $`h_m\prime\prime\prime`$ which is the third most similar historical news to $`n_{t,m}`$
$`S_m\prime\prime\prime`$:denote the similarity score between $`n_{t,m}`$ and $`h_m\prime\prime\prime`$,

$`t_m\prime\prime\prime`$: denote the next trading day of the historical news $`h_m\prime\prime\prime`$ 

$`P_{t_m}\prime\prime\prime `$: denote the Backward Adjusted openig price percentage change on date $`t_m\prime\prime\prime`$ 

3. **Opening Price Prediction**:

To predict the next day opening price percentage change of the closing price of the stock, we will use the weighted average of the historical market performance. Let $`P_{t+1}`$ be the predicted next day opening price percentage change of the closing price of the stock. 


Calculate $`P_{t+1}`$ as the weighted average of the historical market performance $`P_{t_1\prime}, P_{t_1\prime\prime}, P_{t_1\prime\prime\prime},P_{t_2\prime}, P_{t_2\prime\prime}, P_{t_2\prime\prime\prime},\dots P_{t_m\prime}, P_{t_m\prime\prime}, P_{t_m\prime\prime\prime}`$ with weights $`S_1\prime, S_1\prime\prime, S_1\prime\prime\prime,S_2\prime, S_2\prime\prime, S_2\prime\prime\prime, \dots S_m\prime, S_m\prime\prime, S_m\prime\prime\prime`$ respectively:

$`P_{t+1} = \frac{\sum_{i=1}^{m} (S_i\prime P_{t_i\prime} + S_i\prime\prime P_{t_i\prime\prime} + S_i\prime\prime\prime P_{t_i\prime\prime\prime})}{\sum_{i=1}^{m} (S_i\prime + S_i\prime\prime + S_i\prime\prime\prime)}`$


## 4. Methodology and Approach

### Data Collection

- **Financial News Data**: Acquire daily financial news articles from Chinese mainstream media outlets over the past five years.
- **Stock Market Data**: Gather daily stock prices and related data for all listed stocks on the SSE and SZSE during the same period.
```python
import tushare as ts
pro = ts.pro_api(token="")
if __name__ == '__main__':
   
    df_stocks = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
    df_news = pro.news(src='sina', start_date='2018-11-21 09:00:00', end_date='2018-11-22 10:10:00')
```
### Data Cleaning

- **Time Alignment**: Ensure that only news published after 3:00 PM on the previous trading day is used to predict the market performance at 9:30 AM on the current day.

### News Similarity Search Preparation
1. **Convert Historical News to Embeddings**:
   - Collect all historical financial news articles.
   - Use OpenAI's Embedding API to convert each news article into a numerical vector representation (embedding).
   - Ensure that each embedding accurately captures the semantic meaning of the corresponding news article.

2. **Store Embeddings in Vector Database**:
   - Choose a suitable vector database, such as Zilliz, for efficient storage and retrieval of embeddings.
   - Create a schema in the vector database to store the embeddings along with metadata such as the publication date and source of each news article.
   - Insert all the generated embeddings into the vector database, ensuring that each embedding is indexed properly for fast similarity search.

3. **Indexing and Optimization**:
   - Optimize the vector database by creating appropriate indexes to speed up similarity searches.
   - Regularly update the database with new embeddings as new financial news articles are published.

### 
  - follow the steps listed in math formulation,for each news, retrieve the top three most similar historical news from zilliz vector database,record the date and similarity score.
  - Record the market performance at the corresponding historical date.
  - 

### outcome
  - by read the given stock code as parameter, return the prediction of the next date performance. 

## 5. model evaluation
   - For all stock codes from the SSE and SZSE, for each stock, for each date in the past 6 months, use the model to predict the next day backward adjustment opening price percentage change $`P_{t+1}`$. 
   - Compare the prediction $`P_{t+1}`$ with the actual value $`A_{t+1}`$.
   - Calculate the mean squared error (MSE) of the model using the formula:
   
   $`\text{MSE} = \frac{1}{n} \sum_{i=1}^{n} (P_{t+1,i} - A_{t+1,i})^2`$
   
   where $`n`$ is the number of predictions, $`P_{t+1,i}`$ is the predicted value for the $`i`$-th instance, and $`A_{t+1,i}`$ is the actual value for the $`i`$-th instance.
   - Calculate the MSE for each date, aim to find weather there is a time effect that change the prediction accuracy.
   - Calculate the MSE by industry, aim to find weather there is a industry effect that change the prediction accuracy.
   - Calculate the MSE by market value, aim to find weather there is a market value effect that change the prediction accuracy.
