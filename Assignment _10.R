#Import Library
library(ggplot2)
library(dplyr)
library(lubridate)
library(tidyr)
library(DT)
library(ggthemes)
library(wordcloud)
library(tm)
library(SnowballC)

#India_Video$publish_time <- ydm(substr(India_Video$publish_time, start = 1, stop = 10))

#a.Read the YouTube stat from locations = CA, FR, GB, IN, US and prepare the data. 

CAvideos_df <- read.csv("E:/R/CAvideos.csv")
FRvideos_df <- read.csv("E:/R/FRvideos.csv")
GBvideos_df <- read.csv("E:/R/GBvideos.csv")
INvideos_df <- read.csv("E:/R/INvideos.csv")
USvideos_df <- read.csv("E:/R/USvideos.csv")

CA_Video = tail(read.csv("E:/R/CAvideos.csv",encoding = "UTF-8"),20000)

head(CA_Video)

CA_Video$trending_date <- ydm(CA_Video$trending_date)
CA_Video$publish_time <- ydm(substr(CA_Video$publish_time, 
                                    start = 0, stop = 9))
tail(CA_Video)


FR_Video = tail(read.csv("E:/R/FRvideos.csv",encoding = "UTF-8"),20000)

head(FR_Video)

FR_Video$trending_date <- ydm(FR_Video$trending_date)
FR_Video$publish_time <- ydm(substr(FR_Video$publish_time, 
                                    start = 0, stop = 9))
tail(FR_Video)


GB_Video = tail(read.csv("E:/R/GBvideos.csv",encoding = "UTF-8"),20000)

head(GB_Video)

GB_Video$trending_date <- ydm(GB_Video$trending_date)
GB_Video$publish_time <- ydm(substr(GB_Video$publish_time, 
                                    start = 1, stop = 10))
tail(GB_Video)


IN_Video = tail(read.csv("E:/R/INvideos.csv",encoding = "UTF-8"),20000)

head(IN_Video)

IN_Video$trending_date <- ydm(IN_Video$trending_date)
IN_Video$publish_time <- ydm(substr(IN_Video$publish_time, 
                                    start = 0, stop = 9))
tail(IN_Video)

US_Video = tail(read.csv("E:/R/USvideos.csv",encoding = "UTF-8"),20000)

head(US_Video)

US_Video$trending_date <- ydm(US_Video$trending_date)
US_Video$publish_time <- ydm(substr(US_Video$publish_time, 
                                    start = 0, stop = 8))
tail(US_Video)



head(youtube_videos)

youtube_videos <- rbind(CAvideos_df,FRvideos_df,GBvideos_df,INvideos_df,USvideos_df)
youtube_videos$trending_date <- ydm(youtube_videos$trending_date)
youtube_videos$publish_time <- ydm(substr(youtube_videos$publish_time, 
                                          start = 0, stop = 8))
tail(youtube_videos)


#b.Display the correlation plot between category_id,
#views, likes, dislikes, comment_count. Which two have stronger and weaker correlation
youtube <- youtube_videos[, 8:11]
groups <- youtube_videos[, 5]
head(youtube)

pairs(youtube, labels = colnames(youtube),
      pch = 21,
      bg = rainbow(3)[groups],
      col = rainbow(3)[groups])

library(corrplot)

corrplot(cor(youtube), method = 'number')
corrplot(cor(youtube), method = 'color')
corrplot(cor(youtube), method = 'pie')

#c.Display Top 10 most viewed videos of YouTube.

mostviewed <- head(youtube_videos %>%
                     group_by(video_id,title)%>%
                     dplyr::summarise(Total = sum(views)) %>%
                     arrange(desc(Total)),10)
datatable(mostviewed)


ggplot(mostviewed, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = "red") + 
  ggtitle("Top 10 most viewed videos")

#d.Show Top 10 most liked videos on YouTube.
mostliked <- head(youtube_videos %>%
                    group_by(video_id,title)%>%
                    dplyr::summarise(Total = sum(likes)) %>%
                    arrange(desc(Total)),10)
datatable(mostliked)


ggplot(mostliked, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = "red") + 
  ggtitle("Top 10 most viewed videos")


#e.Show Top 10 most disliked videos on YouTube

mostdisliked <- head(youtube_videos %>%
                       group_by(video_id,title)%>%
                       dplyr::summarise(Total = sum(dislikes)) %>%
                       arrange(desc(Total)),10)
datatable(mostdisliked)


ggplot(mostdisliked, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = "red") + 
  ggtitle("Top 10 most viewed videos")

#f.Show Top 10 most commented video of YouTube

mostcommented <- head(youtube_videos %>%
                        group_by(video_id,title)%>%
                        dplyr::summarise(Total = sum(comment_count)) %>%
                        arrange(desc(Total)),10)
datatable(mostcommented)


ggplot(mostcommented, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = "red") + 
  ggtitle("Top 10 most viewed videos")

#G.Show Top 15 videos with maximum percentage (%) of Likes on basis of views on video.
max_likes <- head(youtube_videos %>%
                    group_by(video_id,title)%>% 
                    dplyr::summarise(Total =
                                       round(100 * max(likes, na.rm = T)/max(views, na.rm = T))) %>%
                    arrange(desc(Total)),15)
datatable(max_likes)
ggplot(mostcommented, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = "red") + 
  ggtitle("Top 15 videos with maximum percentage (%) of Likes")

#H.Show Top 15 videos with maximum percentage (%) of Dislikes on basis of views on video.
max_dislikes <- head(youtube_videos %>%
                       group_by(video_id,title)%>% 
                       dplyr::summarise(Total =
                                          round(100 * max(likes, na.rm = T)/max(dislikes, na.rm = T))) %>%
                       arrange(desc(Total)),15)
datatable(max_dislikes)
ggplot(mostcommented, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = "green") + 
  ggtitle("Top 15 videos with maximum percentage (%) of DisLikes")

# i Show Top 15 videos with maximum percentage (%) of Comments on basis of views on video.

mostcommented <- head(youtube_videos %>%
                        group_by(video_id,title) %>%
                        dplyr ::summarise(Total =
                        round(100 * max(likes, na.rm = T)/max(dislikes, na.rm = T)))) %>%
  arrange(desc(Total), 15)
datatable(mostcommented)

ggplot(mostcommented, aes(video_id, Total)) +
  geom_bar( stat = "identity", fill = "red") +
  ggtitle(" Top 15 videos with maximum percentage (%) of comments")

# j. Top trending YouTube channels in all countries
trending_channel <- head(youtube_videos %>%
                           group_by(channel_title,video_id)%>%
                           dplyr::summarise(trending = sum(views)) %>%
                           arrange(desc(trending)),10)
datatable(channel_title)

ggplot(trending_channel, aes(video_id,trending)) +
  geom_bar(stat = "identity", fill = "green") +
  ggtitle("Trending youtube channels in all countries")

##k.Top trending YouTube channels in India.
trending_channel <- head(IN_video %>%
                           group_by(youtube_videos,video_id)%>%
                           dplyr::summarise(trending = sum(views)) %>%
                           arrange(desc(trending)),10)
datatable(youtube_videos)

ggplot(youtube_videos, aes(video_id,trending)) +
  geom_bar(stat = "identity", fill = "green") +
  ggtitle("Trending youtube channels in India")

#L Create A Youtube Titile 

##m.	Show Top Category ID

category <- youtube_videos %>%
  select(category_id,likes) %>%
  group_by(category_id,likes) %>%
  dplyr::summarise(Total = sum(likes)) %>%
  arrange(desc(Total))%>%
  head(n =10L)
## bar
ggplot(category, aes(category_id, Total)) +
  geom_bar( stat = "identity", fill = "red") + 
  ggtitle("Top 10 Category")
## Box 
ggplot(category, aes(category_id,Total))+
  geom_boxplot(fill = "orangered") +
  scale_y_continuous() +
  ggtitle("Top category")

# n. How much time passes between published and trending?

published_trending <- head(youtube_videos %>%
                             group_by(video_id) %>%
                             dplyr::summarise(Result = difftime(publish_time, trending_date, units = "days")) %>%
                             arrange(Result), 20)

head(published_trending, 20)

datatable(published_trending)

ggplot(published_trending, aes(video_id, Result)) +
  geom_bar( stat = "identity", fill = "red") + 
  ylab("Difference") +
  scale_y_continuous() +
  ggtitle("Time Passes Between Published and Trending Date")



##o.Show the relationship plots between viewa vs likes on youtube
plot(x = youtube_videos$views , y=youtube_videos$likes,
     pch = 16, col = "pink",
     xlab = "views",
     ylab = "likes",
     main = "scatter plot")


# p. Top Countries In total number of Views in absolute numbers

countries <- c("CA_Video","FR_Video","GB_Video","IN_Video","US_Video")
views <- c(sum(CA_Video$views), sum(FR_Video$views), sum(GB_Video$views), 
           sum(IN_Video$views), sum(US_Video$views))
likes <- c(sum(CA_Video$likes), sum(FR_Video$likes), sum(GB_Video$likes), 
           sum(IN_Video$likes), sum(US_Video$likes))
dislikes <- c(sum(CA_Video$dislikes), sum(FR_Video$dislikes), sum(GB_Video$dislikes), 
              sum(IN_Video$dislikes), sum(US_Video$dislikes))
comments <- c(sum(CA_Video$comment_count), sum(FR_Video$comment_count), sum(GB_Video$comment_count), 
              sum(IN_Video$comment_count), sum(US_Video$comment_count))

Top_countries <- data.frame(countries, views, likes, dislikes, comments)
Top_countries <- head(Top_countries %>%
                        group_by(countries) %>%
                        arrange(desc(views)))
datatable(Top_countries)

ggplot(Top_countries, aes(x=countries, y=views)) +
  geom_bar( stat = "identity", fill = "deeppink") +
  ggtitle("Top countries In Total Number of views")
# q. Top Countries In total number of Likes in absolute numbers

top_like_country <- head(Top_countries %>%
                           group_by(countries) %>%
                           arrange(desc(likes)))
datatable(top_like_country)

ggplot(top_like_country, aes(x=countries, y=likes)) +
  geom_bar( stat = "identity", fill = "blue") +
  ggtitle("Top Countries In Total Number of Likes")

# q. Top Countries In total number of Likes in absolute numbers

top_like_country <- head(Top_countries %>%
                           group_by(countries) %>%
                           arrange(desc(likes)))
datatable(top_like_country)

ggplot(top_like_country, aes(x=countries, y=likes)) +
  geom_bar( stat = "identity", fill = "green") +
  ggtitle("Top Countries In Total Number of Likes")

# r. Top Countries In total number of Dislikes in absolute numbers

top_dislike_country <- head(Top_countries %>%
                              group_by(countries) %>%
                              arrange(desc(dislikes)))
datatable(top_dislike_country)

ggplot(top_dislike_country, aes(x=countries, y=dislikes)) +
  geom_bar( stat = "identity", fill = "deeppink") +
  ggtitle("Top Countries In Total Number of Dislikes")

# s. Top Countries In total number of Comments in absolute numbers

top_comment_country <- head(Top_countries %>%
                              group_by(countries) %>%
                              arrange(desc(comments)))
datatable(top_comment_country)

ggplot(top_comment_country, aes(x=countries, y=comments)) +
  geom_bar( stat = "identity", fill = "deeppink") +
  ggtitle("Top Countries In Total Number of Comments")


#T.Title length words Frequency Distribution 
freq_dis <- youtube_videos %>%
  group_by(title) %>%
  summarise(counts = n())
datatable(freq_dis)
