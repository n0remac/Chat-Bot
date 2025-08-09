package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Axis interface {
	Run(ctx context.Context, input AxisInput) AxisOutput
	Name() string
}

type AxisInput struct {
	UserInput    string
	Character    *CharacterSheet
	RecentMemory []PostToEmbed
}

type AxisOutput struct {
	Axis      string
	Score     int
	Reason    string
	Timestamp time.Time
}

func RunImmediateAxes(ctx context.Context, input AxisInput, axes []Axis) []AxisOutput {
	var wg sync.WaitGroup
	resultsCh := make(chan AxisOutput, len(axes))

	for _, axis := range axes {
		wg.Add(1)
		go func(ax Axis) {
			defer wg.Done()
			output := ax.Run(ctx, input)
			resultsCh <- output
		}(axis)
	}

	wg.Wait()
	close(resultsCh)

	var results []AxisOutput
	for result := range resultsCh {
		results = append(results, result)
	}
	return results
}

type BackgroundProcessor struct {
	Axes         []Axis
	InputStream  chan AxisInput
	OutputStream chan AxisOutput
	Interval     time.Duration
}

func NewBackgroundProcessor(axes []Axis, interval time.Duration) *BackgroundProcessor {
	return &BackgroundProcessor{
		Axes:         axes,
		InputStream:  make(chan AxisInput, 10),
		OutputStream: make(chan AxisOutput, 100),
		Interval:     interval,
	}
}

func (bp *BackgroundProcessor) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case input := <-bp.InputStream:
				bp.runAxes(ctx, input)
			case <-ctx.Done():
				close(bp.OutputStream)
				return
			case <-time.After(bp.Interval):
				// You could also run axes periodically with the last known context
			}
		}
	}()
}

func (bp *BackgroundProcessor) runAxes(ctx context.Context, input AxisInput) {
	var wg sync.WaitGroup

	for _, axis := range bp.Axes {
		wg.Add(1)
		go func(ax Axis) {
			defer wg.Done()
			output := ax.Run(ctx, input)
			bp.OutputStream <- output
		}(axis)
	}

	wg.Wait()
}

type RecallAxis struct {
	ChannelID     string
	CharacterName string
}

func (r *RecallAxis) Name() string { return "recall" }

func (r *RecallAxis) Run(ctx context.Context, input AxisInput) AxisOutput {
	fmt.Printf("[RecallAxis] Running recall for channel=%s character=%s\n", r.ChannelID, r.CharacterName)
	// Here you can use input.UserInput, input.Character, etc.
	recalled := RecallRelevantPosts(r.ChannelID, r.CharacterName, input.UserInput)
	fmt.Printf("[RecallAxis] Recalled %d posts\n", len(recalled))
	reason := "No relevant posts found"
	if len(recalled) > 0 {
		reason = fmt.Sprintf("Top recall: %s", recalled[0].Message)
	}
	return AxisOutput{
		Axis:      "recall",
		Score:     len(recalled), // or you could define a smarter score
		Reason:    reason,
		Timestamp: time.Now(),
	}
}
