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
	Register("ProjectRecommend", NewProjectRecommendTask())
}

type ProjectRecommendTask struct {
	Workers           int
	ProjectUserMap    map[int64]map[int64][]int64 // 项目关注/加入用户信息
	ProjectIdeaMap    map[int64]map[int64][]int64 // 项目创意信息
	ProjectCommentMap map[int64]map[int64][]int64 // 项目评论信息
	ProjectTitleMap   map[int64]string            // 项目标题信息
	Mutex             sync.Mutex
}

func NewProjectRecommendTask() *ProjectRecommendTask {
	return &ProjectRecommendTask{
		Workers:           1,
		ProjectUserMap:    make(map[int64]map[int64][]int64),
		ProjectIdeaMap:    make(map[int64]map[int64][]int64),
		ProjectCommentMap: make(map[int64]map[int64][]int64),
		ProjectTitleMap:   make(map[int64]string),
	}
}

func (this *ProjectRecommendTask) DoJob(job Job) {
	// id \t title \t user_id \t created_at
	fields := strings.Split(job.data, "\t")
	if len(fields) != 4 {
		return
	}

	id := fields[0]
	title := fields[1]
	user_id := fields[2]
	created_at := fields[3]

	if title == "" || user_id == "" || created_at == "" {
		return
	}

	projectId, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return
	}

	this.Mutex.Lock()
	this.ProjectTitleMap[projectId] = title
	defer this.Mutex.Unlock()

	job.result <- Result{job.data}
}

func (this *ProjectRecommendTask) AddJobs(jobs chan<- Job, result chan<- Result, input *os.File) {
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

func (this *ProjectRecommendTask) DoJobs(done chan<- struct{}, jobs <-chan Job) {
	// 在channel中取出任务并计算
	for job := range jobs {
		this.DoJob(job)
	}

	// 所有工作任务完成后的结束标志
	done <- struct{}{}
}

func (this *ProjectRecommendTask) AwaitJobDone(done <-chan struct{}, result chan<- Result) {
	for i := 0; i < this.Workers; i++ {
		<-done
	}

	// 关闭result channel
	close(result)
}

func (this *ProjectRecommendTask) DoResult(result <-chan Result, output *os.File) {
	var number int = 0
	for _ = range result {
		number += 1

		if number%10000 == 0 {
			fmt.Printf("%d\t%s\n", number, time.Now().String())
		}
	}

	var projectRecommends []model.ProjectRecommend
	minFilterTime := time.Now().AddDate(0, 0, (-1)*util.ProjectRecommendFilterDayNum).Unix()
	for k, v := range this.ProjectTitleMap {
		// 获取用户创意数，最近的创意数量
		ideasCount, recentIdeasCount := this.DoCalculateCount(this.ProjectIdeaMap, k, minFilterTime)

		// 获取用户评论数，近期的评论数量
		commentsCount, recentCommentsCount := this.DoCalculateCount(this.ProjectCommentMap, k, minFilterTime)

		// 获取用户关注/参加数量，最近的用户关注/参加数量
		usersCount, recentUsersCount := this.DoCalculateCount(this.ProjectUserMap, k, minFilterTime)

		// 计算项目得分
		score := util.ProjectRecommendBasicPercent*(float64)(ideasCount+commentsCount+usersCount) + util.ProjectRecommendActionPercent*(float64)(recentIdeasCount+recentCommentsCount+recentUsersCount)

		// data := fmt.Sprintf("%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%f\n", k, v, ideasCount, recentIdeasCount, commentsCount, recentCommentsCount, usersCount, recentUsersCount, score)
		// fmt.Println(data)

		var projectRecommend model.ProjectRecommend
		projectRecommend.Id = k
		projectRecommend.Title = v
		projectRecommend.Score = score

		projectRecommends = append(projectRecommends, projectRecommend)
	}

	util.DescByField(projectRecommends, "Score")
	projectRecommends = projectRecommends[:util.MaxProjectRecommendCount]

	for _, v := range projectRecommends {
		data, err := json.Marshal(&v)
		if err != nil {
			continue
		}

		data = append(data, '\n')
		_, _ = output.WriteString(string(data))
	}
}

func (this *ProjectRecommendTask) DoProcessIdeaFile(inputFile string) error {
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
			projectIdNum, err := strconv.ParseInt(projectId, 10, 64)
			if err != nil {
				continue
			}

			userIdNum, err := strconv.ParseInt(userId, 10, 64)
			if err != nil {
				continue
			}

			createdAtTime := util.ParseDateTime(createdAt).Unix()

			if v, ok := this.ProjectIdeaMap[projectIdNum]; ok {
				v[userIdNum] = append(v[userIdNum], createdAtTime)
			} else {
				var times []int64
				times = append(times, createdAtTime)
				subMap := make(map[int64][]int64)
				subMap[userIdNum] = times
				this.ProjectIdeaMap[projectIdNum] = subMap
			}
		}
	}

	return nil
}

func (this *ProjectRecommendTask) DoProcessCommentFile(inputFile string) error {
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
			projectIdNum, err := strconv.ParseInt(projectId, 10, 64)
			if err != nil {
				continue
			}

			userIdNum, err := strconv.ParseInt(userId, 10, 64)
			if err != nil {
				continue
			}

			createdAtTime := util.ParseDateTime(createdAt).Unix()

			if v, ok := this.ProjectCommentMap[projectIdNum]; ok {
				v[userIdNum] = append(v[userIdNum], createdAtTime)
			} else {
				var times []int64
				times = append(times, createdAtTime)
				subMap := make(map[int64][]int64)
				subMap[userIdNum] = times
				this.ProjectCommentMap[projectIdNum] = subMap
			}
		}
	}

	return nil
}

func (this *ProjectRecommendTask) DoProcessUserProjectRelationFile(inputFile string) error {
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

			// id \t project_id \t user_id \t status \t created_at
			fields := strings.Split(realLine, "\t")
			if len(fields) != 5 {
				continue
			}

			projectId := fields[1]
			userId := fields[2]
			status := fields[3]
			createdAt := fields[4]

			if projectId == "" || userId == "" || createdAt == "" {
				continue
			}

			// 查看是否为关注/加入状态
			if status != "follow" && status != "join" {
				continue
			}

			var err error
			projectIdNum, err := strconv.ParseInt(projectId, 10, 64)
			if err != nil {
				continue
			}

			userIdNum, err := strconv.ParseInt(userId, 10, 64)
			if err != nil {
				continue
			}

			createdAtTime := util.ParseDateTime(createdAt).Unix()

			if v, ok := this.ProjectUserMap[projectIdNum]; ok {
				v[userIdNum] = append(v[userIdNum], createdAtTime)
			} else {
				var times []int64
				times = append(times, createdAtTime)
				subMap := make(map[int64][]int64)
				subMap[userIdNum] = times
				this.ProjectUserMap[projectIdNum] = subMap
			}
		}
	}

	return nil
}

func (this *ProjectRecommendTask) DoCalculateCount(data map[int64]map[int64][]int64, key int64, minFilterTime int64) (int64, int64) {
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

func (this *ProjectRecommendTask) DoDataTask(inputFiles []string, outputFile string, arg interface{}) error {
	// 检查输入参数信息
	if len(inputFiles) < 4 {
		return errors.New("DoDataTask ProjectRecommendTask check fail, inputFiles len is not correct")
	}

	// 设置文件名称
	projectFile := inputFiles[0]
	ideaFile := inputFiles[1]
	commentFile := inputFiles[2]
	userProjectRelationFile := inputFiles[3]

	var err error
	// 处理项目创意信息
	err = this.DoProcessIdeaFile(ideaFile)
	if err != nil {
		return err
	}

	// 处理项目评论信息
	err = this.DoProcessCommentFile(commentFile)
	if err != nil {
		return err
	}

	// 处理项目用户关系信息
	err = this.DoProcessUserProjectRelationFile(userProjectRelationFile)
	if err != nil {
		return err
	}

	// 读取输入文件
	input, err := os.OpenFile(projectFile, os.O_RDONLY, 0)
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
