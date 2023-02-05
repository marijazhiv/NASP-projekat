package configurations

import (
	"encoding/json"
	"io/ioutil"
)

type Write_Ahead_Log_Config struct {
	Segment_Capacity int `json:"segment_capacity"`
}

type Cache_Config struct {
	Max_Data int `json:"max_data"`
}

type Log_Structured_Merge_Tree_Config struct {
	Max_Level  int `json:"max_level"`
	Level_Size int `json:"level_size"`
}

type Token_Bucket_Config struct {
	Max_Tokens int `json:"max_tokens"`
	Interval   int `json:"interval"`
}

type Mem_Table_Config struct {
	Skip_List_Max_Height int `json:"skip_list_max_height"`
	Max_Size             int `json:"max_size"`
	Limit                int `json:"limit"`
}

type Hyper_Log_Log_Config struct {
	Precision int `json:"precision"`
}

type Count_Min_Sketch_Config struct {
	Precision float64 `json:"precision"`
	Accuracy  float64 `json:"accuracy"`
}

type Config struct {
	WAL_Parameters         Write_Ahead_Log_Config           `json:"write_ahead_log"`
	Cache_Parameters       Cache_Config                     `json:"cache"`
	LSM_Parameters         Log_Structured_Merge_Tree_Config `json:"log_structured_merge_tree"`
	TokenBucket_Parameters Token_Bucket_Config              `json:"token_bucket"`
	MemTable_Parameters    Mem_Table_Config                 `json:"mem_table"`
	HLL_Parameters         Hyper_Log_Log_Config             `json:"hyper_log_log"`
	CSM_Parameters         Count_Min_Sketch_Config          `json:"count_min_sketch"`
}

func Get_Configurations() (config *Config) {
	config = new(Config)

	jsonBytes, err := ioutil.ReadFile("configurations/configurations.json")
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(jsonBytes, config)
	if err != nil {
		panic(err)
	}

	if config.WAL_Parameters.Segment_Capacity == -1 {
		config.WAL_Parameters.Segment_Capacity = 50
	}

	if config.Cache_Parameters.Max_Data == -1 {
		config.Cache_Parameters.Max_Data = 5
	}

	if config.LSM_Parameters.Max_Level == -1 {
		config.LSM_Parameters.Max_Level = 4
	}
	if config.LSM_Parameters.Level_Size == -1 {
		config.LSM_Parameters.Level_Size = 2
	}

	if config.TokenBucket_Parameters.Interval == -1 {
		config.TokenBucket_Parameters.Interval = 100
	}
	if config.TokenBucket_Parameters.Max_Tokens == -1 {
		config.TokenBucket_Parameters.Max_Tokens = 1000
	}

	if config.MemTable_Parameters.Limit == -1 {
		config.MemTable_Parameters.Limit = 60
	}
	if config.MemTable_Parameters.Max_Size == -1 {
		config.MemTable_Parameters.Max_Size = 5
	}
	if config.MemTable_Parameters.Skip_List_Max_Height == -1 {
		config.MemTable_Parameters.Skip_List_Max_Height = 5
	}

	if config.HLL_Parameters.Precision == -1 {
		config.HLL_Parameters.Precision = 4
	}

	if config.CSM_Parameters.Accuracy == -1 {
		config.CSM_Parameters.Accuracy = 0.01
	}
	if config.CSM_Parameters.Precision == -1 {
		config.CSM_Parameters.Precision = 0.1
	}

	return
}

func Create_Configurations_File() {
	config := new(Config)

	config.WAL_Parameters.Segment_Capacity = -1

	config.Cache_Parameters.Max_Data = -1

	config.LSM_Parameters.Level_Size = -1
	config.LSM_Parameters.Max_Level = -1

	config.TokenBucket_Parameters.Interval = -1
	config.TokenBucket_Parameters.Max_Tokens = -1

	config.MemTable_Parameters.Limit = -1
	config.MemTable_Parameters.Max_Size = -1
	config.MemTable_Parameters.Skip_List_Max_Height = -1

	config.HLL_Parameters.Precision = -1

	config.CSM_Parameters.Accuracy = -1
	config.CSM_Parameters.Precision = -1

	file, _ := json.MarshalIndent(config, "", "  ")

	_ = ioutil.WriteFile("configurations/configurations.json", file, 0644)
}
