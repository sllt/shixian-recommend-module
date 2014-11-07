package task

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"doraemon/model"
	"doraemon/util"
)

func init() {
	Register("UserRecommend", NewUserRecommendTask())
}

type UserRecommendTask struct {
	Workers         int
	UserRalationMap map[int64]map[int64][]int64 // 用户被关注信息
	UserIdeaMap     map[int64][]int64           // 用户创意信息
	UserCommentMap  map[int64][]int64           // 用户评论信息
	UserInfoMap     map[int64]*model.User       // 用户基本信息
	Mutex           sync.Mutex
}

func NewUserRecommendTask() *UserRecommendTask {
	return &UserRecommendTask{
		Workers:         1,
		UserRalationMap: make(map[int64]map[int64][]int64),
		UserIdeaMap:     make(map[int64][]int64),
		UserCommentMap:  make(map[int64][]int64),
		UserInfoMap:     make(map[int64]*model.User),
	}
}

func (this *UserRecommendTask) DoJob(job Job) {
	// id \t username \t last_sign_in_at \t created_at
	fields := strings.Split(job.data, "\t")
	if len(fields) != 4 {
		return
	}

	id := fields[0]
	username := fields[1]
	last_sign_in_at := fields[2]
	created_at := fields[3]

	if username == "" || last_sign_in_at == "" || created_at == "" {
		return
	}

	userId, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return
	}

	this.Mutex.Lock()
	if v, ok := this.UserInfoMap[userId]; ok {
		v.Name = username
		v.Id = userId
		v.LastSignInAt = util.ParseDateTime(last_sign_in_at).Unix()
	} else {
		user := &model.User{}
		user.Id = userId
		user.Name = username
		user.LastSignInAt = util.ParseDateTime(last_sign_in_at).Unix()
		this.UserInfoMap[userId] = user
	}
	defer this.Mutex.Unlock()

	job.result <- Result{job.data}
}

func (this *UserRecommendTask) AddJobs(jobs chan<- Job, result chan<- Result, input *os.File) {
	br := bufio.NewReader(input)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			realLine := strings.ToLower(strings.TrimRight(line, "\n"))
			jobs <- Job{realLine, result}
		}
	}

	// 关闭job channel
	close(jobs)
}

func (this *UserRecommendTask) DoJobs(done chan<- struct{}, jobs <-chan Job) {
	// 在channel中取出任务并计算
	for job := range jobs {
		this.DoJob(job)
	}

	// 所有工作任务完成后的结束标志
	done <- struct{}{}
}

func (this *UserRecommendTask) AwaitJobDone(done <-chan struct{}, result chan<- Result) {
	for i := 0; i < this.Workers; i++ {
		<-done
	}

	// 关闭result channel
	close(result)
}

func (this *UserRecommendTask) DoResult(result <-chan Result, output *os.File) {
	var number int = 0
	for _ = range result {
		number += 1

		if number%10000 == 0 {
			fmt.Printf("%d\t%s\n", number, time.Now().String())
		}
	}

	var userRecommends []model.UserRecommend
	minFilterTime := time.Now().AddDate(0, 0, (-1)*util.UserRecommendFilterDayNum).Unix()
	for k, v := range this.UserInfoMap {
		// 获取用户创意数，最近的创意数量
		ideasCount, recentIdeasCount := this.DoCalculateCount(this.UserIdeaMap, k, minFilterTime)

		// 获取用户评论数，近期的评论数量
		commentsCount, recentCommentsCount := this.DoCalculateCount(this.UserCommentMap, k, minFilterTime)

		// 获取用户关注数量，最近的用户关注数量
		usersCount, recentUsersCount := this.DoCalculateCount2(this.UserRalationMap, k, minFilterTime)

		// 计算项目得分
		score := util.UserRecommendBasicPercent*(float64)(ideasCount+commentsCount+usersCount) + util.UserRecommendActionPercent*(float64)(recentIdeasCount+recentCommentsCount+recentUsersCount)

		// data := fmt.Sprintf("%d\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%f\n", k, v.Name, v.Description, ideasCount, recentIdeasCount, commentsCount, recentCommentsCount, usersCount, recentUsersCount, score)
		// fmt.Println(data)

		var userRecommend model.UserRecommend
		userRecommend.Id = k
		userRecommend.Name = v.Name
		userRecommend.Description = v.Description
		userRecommend.Score = score

		userRecommends = append(userRecommends, userRecommend)
	}

	util.DescByField(userRecommends, "Score")
	userRecommends = userRecommends[:util.MaxUserRecommendCount]

	for _, v := range userRecommends {
		data, err := json.Marshal(&v)
		if err != nil {
			continue
		}

		data = append(data, '\n')
		_, _ = output.WriteString(string(data))
	}
}

func (this *UserRecommendTask) DoProcessIdeaFile(inputFile string) error {
	input, err := os.OpenFile(inputFile, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer input.Close()

	br := bufio.NewReader(input)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			realLine := strings.ToLower(strings.TrimRight(line, "\n"))

			// id \t project_id \t user_id \t created_at
			fields := strings.Split(realLine, "\t")
			if len(fields) != 4 {
				continue
			}

			projectId := fields[1]
			userId := fields[2]
			createdAt := fields[3]

			if projectId == "" || userId == "" || createdAt == "" {
				continue
			}

			var err error
			userIdNum, err := strconv.ParseInt(userId, 10, 64)
			if err != nil {
				continue
			}

			createdAtTime := util.ParseDateTime(createdAt).Unix()

			if v, ok := this.UserIdeaMap[userIdNum]; ok {
				v = append(v, createdAtTime)
			} else {
				var times []int64
				times = append(times, createdAtTime)
				this.UserIdeaMap[userIdNum] = times
			}
		}
	}

	return nil
}

func (this *UserRecommendTask) DoProcessCommentFile(inputFile string) error {
	input, err := os.OpenFile(inputFile, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer input.Close()

	br := bufio.NewReader(input)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			realLine := strings.ToLower(strings.TrimRight(line, "\n"))

			// id \t project_id \t user_id \t commentable_id \t commentable_type \t created_at
			fields := strings.Split(realLine, "\t")
			if len(fields) != 6 {
				continue
			}

			projectId := fields[1]
			userId := fields[2]
			createdAt := fields[5]

			if projectId == "" || userId == "" || createdAt == "" {
				continue
			}

			var err error
			userIdNum, err := strconv.ParseInt(userId, 10, 64)
			if err != nil {
				continue
			}

			createdAtTime := util.ParseDateTime(createdAt).Unix()

			if v, ok := this.UserCommentMap[userIdNum]; ok {
				v = append(v, createdAtTime)
			} else {
				var times []int64
				times = append(times, createdAtTime)
				this.UserCommentMap[userIdNum] = times
			}
		}
	}

	return nil
}

func (this *UserRecommendTask) DoProcessUserProfileFile(inputFile string) error {
	input, err := os.OpenFile(inputFile, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer input.Close()

	br := bufio.NewReader(input)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			realLine := strings.ToLower(strings.TrimRight(line, "\n"))

			// id \t user_id \t description \t created_at
			fields := strings.Split(realLine, "\t")
			if len(fields) != 4 {
				continue
			}

			userId := fields[1]
			description := fields[2]
			createdAt := fields[3]

			if userId == "" || description == "" || createdAt == "" {
				continue
			}

			var err error
			userIdNum, err := strconv.ParseInt(userId, 10, 64)
			if err != nil {
				continue
			}

			user := &model.User{}
			user.Id = userIdNum
			user.Description = description
			this.UserInfoMap[userIdNum] = user
		}
	}

	return nil
}

func (this *UserRecommendTask) DoProcessUserRelationFile(inputFile string) error {
	input, err := os.OpenFile(inputFile, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer input.Close()

	br := bufio.NewReader(input)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			realLine := strings.ToLower(strings.TrimRight(line, "\n"))

			// id \t follower_id \t followed_id \t created_at
			fields := strings.Split(realLine, "\t")
			if len(fields) != 4 {
				continue
			}

			followerId := fields[1]
			followedId := fields[2]
			createdAt := fields[3]

			if followerId == "" || followedId == "" || createdAt == "" {
				continue
			}

			var err error
			followerIdNum, err := strconv.ParseInt(followerId, 10, 64)
			if err != nil {
				continue
			}

			followedIdNum, err := strconv.ParseInt(followedId, 10, 64)
			if err != nil {
				continue
			}

			createdAtTime := util.ParseDateTime(createdAt).Unix()

			if v, ok := this.UserRalationMap[followedIdNum]; ok {
				v[followerIdNum] = append(v[followerIdNum], createdAtTime)
			} else {
				var times []int64
				times = append(times, createdAtTime)
				subMap := make(map[int64][]int64)
				subMap[followerIdNum] = times
				this.UserRalationMap[followedIdNum] = subMap
			}
		}
	}

	return nil
}

func (this *UserRecommendTask) DoCalculateCount2(data map[int64]map[int64][]int64, key int64, minFilterTime int64) (int64, int64) {
	var countA, countB int64

	if v, ok := data[key]; ok {
		for _, vv := range v {
			for _, vvv := range vv {
				countA += 1

				if vvv >= minFilterTime {
					countB += 1
				}
			}
		}
	}

	return countA, countB
}

func (this *UserRecommendTask) DoCalculateCount(data map[int64][]int64, key int64, minFilterTime int64) (int64, int64) {
	var countA, countB int64

	if v, ok := data[key]; ok {
		for _, vv := range v {
			countA += 1

			if vv >= minFilterTime {
				countB += 1
			}
		}
	}

	return countA, countB
}

func (this *UserRecommendTask) DoDataTask(inputFiles []string, outputFile string, arg interface{}) error {
	// 检查输入参数信息
	if len(inputFiles) < 5 {
		return errors.New("DoDataTask UserRecommendTask check fail, inputFiles len is not correct")
	}

	// 设置文件名称
	userFile := inputFiles[0]
	userProfileFile := inputFiles[1]
	ideaFile := inputFiles[2]
	commentFile := inputFiles[3]
	userRelationFile := inputFiles[4]

	var err error
	// 处理项目创意信息
	err = this.DoProcessUserProfileFile(userProfileFile)
	if err != nil {
		return err
	}

	// 处理用户创意信息
	err = this.DoProcessIdeaFile(ideaFile)
	if err != nil {
		return err
	}

	// 处理用户评论信息
	err = this.DoProcessCommentFile(commentFile)
	if err != nil {
		return err
	}

	// 处理用户关注信息
	err = this.DoProcessUserRelationFile(userRelationFile)
	if err != nil {
		return err
	}

	// 读取输入文件
	input, err := os.OpenFile(userFile, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer input.Close()

	// 创建生成结果文件
	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer output.Close()

	runtime.GOMAXPROCS(runtime.NumCPU())

	jobs := make(chan Job, this.Workers)
	done := make(chan struct{}, this.Workers)
	result := make(chan Result, this.Workers)

	// 将需要并发处理的任务添加到jobs的channel中
	go this.AddJobs(jobs, result, input)

	// 根据cpu的数量启动对应个数的goroutines从jobs争夺任务进行处理
	for i := 0; i < this.Workers; i++ {
		go this.DoJobs(done, jobs)
	}

	// 等待所有worker routiines的完成结果, 并将结果通知主routine
	go this.AwaitJobDone(done, result)

	this.DoResult(result, output)
	return nil
}
