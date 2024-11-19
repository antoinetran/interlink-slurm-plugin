package slurm

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/log"

	commonIL "github.com/intertwin-eu/interlink/pkg/interlink"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

// Logs in follow mode (get logs until the death of the container) with "kubectl -f".
func (h *SidecarHandler) GetLogsFollowMode(w http.ResponseWriter, r *http.Request, path string, req commonIL.LogStruct, containerOutputPath string, containerOutput []byte, sessionNumber int) error {
	// Follow until this file exist, that indicates the end of container, thus the end of following.
	containerStatusPath := path + "/" + req.ContainerName + ".status"
	// Get the offset of what we read.
	containerOutputLastOffset := len(containerOutput)
	log.G(h.Ctx).Debug("GO routine session " + strconv.Itoa(sessionNumber) + " Read container " + containerStatusPath + " with current length/offset: " + strconv.Itoa(containerOutputLastOffset))

	var containerOutputFd *os.File
	var err error
	for {
		containerOutputFd, err = os.Open(containerOutputPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Case the file does not exist yet, we loop until it exist.
				log.G(h.Ctx).Debug("GO routine session " + strconv.Itoa(sessionNumber) + " Cannot open in follow mode the container logs " + containerOutputPath + " because it does not exist yet, sleeping before retrying...")
				time.Sleep(2 * time.Second)
				continue
			} else {
				// Case unknown error.
				errWithContext := fmt.Errorf("GO routine session "+strconv.Itoa(sessionNumber)+" could not open file to follow logs at %s error type: %s error: %w", containerOutputPath, fmt.Sprintf("%#v", err), err)
				log.G(h.Ctx).Error("GO routine session " + strconv.Itoa(sessionNumber) + " Cannot open in follow mode the container logs " + containerOutputPath + " because it does not exist yet, sleeping before retrying...")
				w.Write([]byte(errWithContext.Error()))
				return err
			}
		}
		// File exist.
		log.G(h.Ctx).Debug("GO routine session " + strconv.Itoa(sessionNumber) + " Opened for follow mode the container logs " + containerOutputPath)
		break
	}
	defer containerOutputFd.Close()

	// We follow only from after what is already read.
	_, err = containerOutputFd.Seek(int64(containerOutputLastOffset), 0)
	if err != nil {
		errWithContext := fmt.Errorf("GO routine session "+strconv.Itoa(sessionNumber)+" error during Seek() of GetLogsFollowMode() in GetLogsHandler of file %s offset %d type: %s %w", containerOutputPath, containerOutputLastOffset, fmt.Sprintf("%#v", err), err)
		w.Write([]byte(errWithContext.Error()))
		return errWithContext
	}

	containerOutputReader := bufio.NewReader(containerOutputFd)

	bufferBytes := make([]byte, 4096)

	// Looping until we get end of job.
	// TODO: handle the Ctrl+C of kubectl logs.
	var isContainerDead bool = false
	for {
		n, err := containerOutputReader.Read(bufferBytes)
		if err != nil {
			if err == io.EOF {
				// Nothing more to read, but in follow mode, is the container still alive?
				if isContainerDead {
					// Container already marked as dead, and we tried to get logs one last time. Exiting the loop.
					log.G(h.Ctx).Info("Session " + strconv.Itoa(sessionNumber) + ": Container is found dead and no more logs are found at this step, exiting following mode...")
					break
				}
				// Checking if container is dead (meaning the status file exist).
				if _, err := os.Stat(containerStatusPath); errors.Is(err, os.ErrNotExist) {
					// The status file of the container does not exist, so the container is still alive. Continuing to follow logs.
					// Sleep because otherwise it can be a stress to file system to always read it when it has nothing.
					log.G(h.Ctx).Debug("Session " + strconv.Itoa(sessionNumber) + ": EOF of container logs, sleeping 4s before retrying...")
					time.Sleep(4 * time.Second)
				} else {
					// The status file exist, so the container is dead. Trying to get the latest log one last time.
					// Because the moment we found the status file, there might be some more logs to read.
					isContainerDead = true
					log.G(h.Ctx).Info("Session " + strconv.Itoa(sessionNumber) + ": Container is found dead, reading last logs...")
				}
				continue
			} else {
				// Error during read.
				return err
			}
		}
		_, err = w.Write(bufferBytes[:n])
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.G(h.Ctx).Error(err)
		}

		// Flush otherwise it will take time to appear in kubectl logs.
		if f, ok := w.(http.Flusher); ok {
			log.G(h.Ctx).Debug("Session " + strconv.Itoa(sessionNumber) + ": Wrote some logs, now flushing...")
			f.Flush()
		} else {
			log.G(h.Ctx).Debug("Session " + strconv.Itoa(sessionNumber) + ": Wrote some logs but could not flush because server does not support Flusher. It means the logs will take time to appear.")
		}

	}
	// No error, err = nil
	return nil
}

func (h *SidecarHandler) logErrorVerbose(context string, ctx context.Context, w http.ResponseWriter, err error) {
	errWithContext := fmt.Errorf("error context: %s type: %s %w", context, fmt.Sprintf("%#v", err), err)
	log.G(h.Ctx).Error(errWithContext)
	statusCode := http.StatusInternalServerError
	h.handleError(ctx, w, statusCode, errWithContext)
}

func (h *SidecarHandler) readLogs(logsPath string, span trace.Span, ctx context.Context, w http.ResponseWriter) ([]byte, error) {
	output, err := os.ReadFile(logsPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Case file does not exist yet. This is normal, returning empty array.
			output = make([]byte, 0)
		} else {
			span.AddEvent("Error retrieving logs")
			h.logErrorVerbose("Error during ReadFile() of readLogs() in GetLogsHandler of file "+logsPath, ctx, w, err)
			return nil, err
		}
	}
	return output, nil
}

// GetLogsHandler reads Jobs' output file to return what's logged inside.
// What's returned is based on the provided parameters (Tail/LimitBytes/Timestamps/etc)
func (h *SidecarHandler) GetLogsHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now().UnixMicro()
	tracer := otel.Tracer("interlink-API")
	spanCtx, span := tracer.Start(h.Ctx, "GetLogsSLURM", trace.WithAttributes(
		attribute.Int64("start.timestamp", start),
	))
	defer span.End()
	defer commonIL.SetDurationSpan(start, span)

	// For debugging purpose, when we have many kubectl logs, we can differentiate each one.
	sessionNumber := rand.Intn(100000)

	log.G(h.Ctx).Info("GO routine session " + strconv.Itoa(sessionNumber) + " Docker Sidecar: received GetLogs call")
	var req commonIL.LogStruct
	statusCode := http.StatusOK
	currentTime := time.Now()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.logErrorVerbose("GO routine session "+strconv.Itoa(sessionNumber)+" Error during ReadAll() in GetLogsHandler request body, status code "+strconv.Itoa(statusCode), spanCtx, w, err)
		return
	}

	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		statusCode = http.StatusInternalServerError
		h.logErrorVerbose("GO routine session "+strconv.Itoa(sessionNumber)+" Error during Unmarshal() in GetLogsHandler request body, status code "+strconv.Itoa(statusCode), spanCtx, w, err)
		return
	}

	span.SetAttributes(
		attribute.String("pod.name", req.PodName),
		attribute.String("pod.namespace", req.Namespace),
		attribute.Int("opts.limitbytes", req.Opts.LimitBytes),
		attribute.Int("opts.since", req.Opts.SinceSeconds),
		attribute.Int64("opts.sincetime", req.Opts.SinceTime.UnixMicro()),
		attribute.Int("opts.tail", req.Opts.Tail),
		attribute.Bool("opts.follow", req.Opts.Follow),
		attribute.Bool("opts.previous", req.Opts.Previous),
		attribute.Bool("opts.timestamps", req.Opts.Timestamps),
	)

	path := h.Config.DataRootFolder + req.Namespace + "-" + req.PodUID
	containerOutputPath := path + "/" + req.ContainerName + ".out"
	var output []byte
	if req.Opts.Timestamps {
		h.logErrorVerbose("GO routine session "+strconv.Itoa(sessionNumber)+" Unsupported option req.Opts.Timestamps", spanCtx, w, err)
		return
	}
	log.G(h.Ctx).Info("GO routine session " + strconv.Itoa(sessionNumber) + " Reading  " + path + "/" + req.ContainerName + ".out")
	containerOutput, err := h.readLogs(containerOutputPath, span, spanCtx, w)
	if err != nil {
		return
	}
	jobOutput, err := h.readLogs(path+"/"+"job.out", span, spanCtx, w)
	if err != nil {
		return
	}

	output = append(output, jobOutput...)
	output = append(output, containerOutput...)

	var returnedLogs string

	if req.Opts.Tail != 0 {
		var lastLines []string

		splittedLines := strings.Split(string(output), "\n")

		if req.Opts.Tail > len(splittedLines) {
			lastLines = splittedLines
		} else {
			lastLines = splittedLines[len(splittedLines)-req.Opts.Tail-1:]
		}

		for _, line := range lastLines {
			returnedLogs += line + "\n"
		}
	} else if req.Opts.LimitBytes != 0 {
		var lastBytes []byte
		if req.Opts.LimitBytes > len(output) {
			lastBytes = output
		} else {
			lastBytes = output[len(output)-req.Opts.LimitBytes-1:]
		}

		returnedLogs = string(lastBytes)
	} else {
		returnedLogs = string(output)
	}

	if req.Opts.Timestamps && (req.Opts.SinceSeconds != 0 || !req.Opts.SinceTime.IsZero()) {
		temp := returnedLogs
		returnedLogs = ""
		splittedLogs := strings.Split(temp, "\n")
		timestampFormat := "2006-01-02T15:04:05.999999999Z"

		for _, Log := range splittedLogs {
			part := strings.SplitN(Log, " ", 2)
			timestampString := part[0]
			timestamp, err := time.Parse(timestampFormat, timestampString)
			if err != nil {
				continue
			}
			if req.Opts.SinceSeconds != 0 {
				if currentTime.Sub(timestamp).Seconds() > float64(req.Opts.SinceSeconds) {
					returnedLogs += Log + "\n"
				}
			} else {
				if timestamp.Sub(req.Opts.SinceTime).Seconds() >= 0 {
					returnedLogs += Log + "\n"
				}
			}
		}
	}

	commonIL.SetDurationSpan(start, span, commonIL.WithHTTPReturnCode(statusCode))

	w.WriteHeader(statusCode)
	w.Write([]byte(returnedLogs))

	if req.Opts.Follow {
		err := h.GetLogsFollowMode(w, r, path, req, containerOutputPath, containerOutput, sessionNumber)
		if err != nil {
			h.logErrorVerbose("GO routine session "+strconv.Itoa(sessionNumber)+" Follow mode error", spanCtx, w, err)
		}
	}
}
