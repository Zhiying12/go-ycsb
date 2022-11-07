package multipaxos

type Command struct {
	Key         string
	Value       string
	CommandType string
}

type CommandResult struct {
	Ok        bool
	Value     string
}