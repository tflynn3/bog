package main

import "syscall"

func (e *candidateEvidence) finish(p *child) {
	if p == nil {
		return
	}
	p.logs.mu.Lock()
	e.LogTail, e.LogTruncated = string(p.logs.tail), p.logs.truncated
	p.logs.mu.Unlock()
	select {
	case <-p.done:
		e.Reaped = true
		if p.logDrainErr != nil {
			e.LogTruncated = true
			e.LogDrainError = p.logDrainErr.Error()
		}
		code := p.cmd.ProcessState.ExitCode()
		status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
		if ok && status.Signaled() {
			e.Signal = status.Signal().String()
		} else if code >= 0 {
			e.ExitCode = &code
		}
		if p.waitErr != nil {
			e.WaitError = p.waitErr.Error()
		}
	default:
	}
}
