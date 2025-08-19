package manager

import (
	"fmt"
	"html/template"
	"log"
	"math/rand"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"maps"

	"github.com/Masterminds/sprig/v3"
	"github.com/knadh/listmonk/internal/i18n"
	"github.com/knadh/listmonk/internal/notifs"
	"github.com/knadh/listmonk/models"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	// BaseTPL is the name of the base template.
	BaseTPL = "base"

	// ContentTpl is the name of the compiled message.
	ContentTpl = "content"

	dummyUUID = "00000000-0000-0000-0000-000000000000"
)

// Store represents a data backend, such as a database,
// that provides subscriber and campaign records.
type Store interface {
	NextCampaigns(currentIDs []int64, sentCounts []int64) ([]*models.Campaign, error)
	NextSubscribers(campID, limit int) ([]models.Subscriber, error)
	GetCampaign(campID int) (*models.Campaign, error)
	GetAttachment(mediaID int) (models.Attachment, error)
	UpdateCampaignStatus(campID int, status string) error
	UpdateCampaignCounts(campID int, toSend int, sent int, lastSubID int) error
	CreateLink(url string) (string, error)
	BlocklistSubscriber(id int64) error
	DeleteSubscriber(id int64) error
}

// Messenger is an interface for a generic messaging backend,
// for instance, e-mail, SMS etc.
type Messenger interface {
	Name() string
	Push(models.Message) error
	Flush() error
	Close() error
}

// CampStats contains campaign stats like per minute send rate.
type CampStats struct {
	SendRate int
}

// Manager handles the scheduling, processing, and queuing of campaigns
// and message pushes.
type Manager struct {
	cfg        Config
	store      Store
	i18n       *i18n.I18n
	messengers map[string]Messenger
	fnNotify   func(subject string, data any) error
	log        *log.Logger

	// Campaigns that are currently running.
	pipes    map[int]*pipe
	pipesMut sync.RWMutex

	tpls    map[int]*models.Template
	tplsMut sync.RWMutex

	// Links generated using Track() are cached here so as to not query
	// the database for the link UUID for every message sent. This has to
	// be locked as it may be used externally when previewing campaigns.
	links    map[string]string
	linksMut sync.RWMutex

	// No more global queues - each campaign will manage its own sending
	// nextPipes chan *pipe  // REMOVED
	// campMsgQ  chan CampaignMessage  // REMOVED
	// msgQ      chan models.Message  // REMOVED

	// No more global sliding window - each campaign manages its own rate limiting
	// slidingCount int  // REMOVED
	// slidingStart time.Time  // REMOVED

	tplFuncs template.FuncMap
}

// CampaignMessage represents an instance of campaign message to be pushed out,
// specific to a subscriber, via the campaign's messenger.
type CampaignMessage struct {
	Campaign   *models.Campaign
	Subscriber models.Subscriber

	from     string
	to       string
	subject  string
	body     []byte
	altBody  []byte
	unsubURL string

	pipe *pipe
}

// Config has parameters for configuring the manager.
type Config struct {
	// Number of subscribers to pull from the DB in a single iteration.
	BatchSize             int
	Concurrency           int
	MessageRate           int
	MaxSendErrors         int
	SlidingWindow         bool
	SlidingWindowDuration time.Duration
	SlidingWindowRate     int
	RequeueOnError        bool
	FromEmail             string
	IndividualTracking    bool
	LinkTrackURL          string
	UnsubURL              string
	OptinURL              string
	MessageURL            string
	ViewTrackURL          string
	ArchiveURL            string
	RootURL               string
	UnsubHeader           bool

	// Random sending interval parameters (in seconds)
	RandomDelayMin int
	RandomDelayMax int

	// Random batch size parameters
	RandomBatchMin int
	RandomBatchMax int

	// Interval to scan the DB for active campaign checkpoints.
	ScanInterval time.Duration

	// ScanCampaigns indicates whether this instance of manager will scan the DB
	// for active campaigns and process them.
	// This can be used to run multiple instances of listmonk
	// (exposed to the internet, private etc.) where only one does campaign
	// processing while the others handle other kinds of traffic.
	ScanCampaigns bool
}

var pushTimeout = time.Second * 3

// New returns a new instance of Mailer.
func New(cfg Config, store Store, i *i18n.I18n, l *log.Logger) *Manager {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 1000
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.MessageRate < 1 {
		cfg.MessageRate = 1
	}

	m := &Manager{
		cfg:   cfg,
		store: store,
		i18n:  i,
		fnNotify: func(subject string, data any) error {
			return notifs.NotifySystem(subject, notifs.TplCampaignStatus, data, nil)
		},
		log:        l,
		messengers: make(map[string]Messenger),
		pipes:      make(map[int]*pipe),
		tpls:       make(map[int]*models.Template),
		links:      make(map[string]string),
		// Removed global queue initialization - each campaign manages its own
	}
	m.tplFuncs = m.makeGnericFuncMap()

	return m
}

// AddMessenger adds a Messenger messaging backend to the manager.
func (m *Manager) AddMessenger(msg Messenger) error {
	id := msg.Name()
	if _, ok := m.messengers[id]; ok {
		return fmt.Errorf("messenger '%s' is already loaded", id)
	}
	m.messengers[id] = msg

	return nil
}

// PushMessage pushes an arbitrary non-campaign Message to be sent out directly.
// Since we removed global queues, non-campaign messages are sent immediately.
func (m *Manager) PushMessage(msg models.Message) error {
	// Send message directly using the appropriate messenger
	msgr, ok := m.messengers[msg.Messenger]
	if !ok {
		return fmt.Errorf("unknown messenger %s", msg.Messenger)
	}
	
	return msgr.Push(msg)
}

// PushCampaignMessage is no longer needed as each campaign manages its own message sending.
// This function is kept for compatibility but does nothing.
func (m *Manager) PushCampaignMessage(msg CampaignMessage) error {
	// No longer used - each campaign sends messages directly
	return nil
}

// HasMessenger checks if a given messenger is registered.
func (m *Manager) HasMessenger(id string) bool {
	_, ok := m.messengers[id]

	return ok
}

// HasRunningCampaigns checks if there are any active campaigns.
func (m *Manager) HasRunningCampaigns() bool {
	m.pipesMut.Lock()
	defer m.pipesMut.Unlock()

	return len(m.pipes) > 0
}

// GetCampaignStats returns campaign statistics.
func (m *Manager) GetCampaignStats(id int) CampStats {
	n := 0

	m.pipesMut.Lock()
	if c, ok := m.pipes[id]; ok {
		n = int(c.rate.Rate())
	}
	m.pipesMut.Unlock()

	return CampStats{SendRate: n}
}

// Run is a blocking function (that should be invoked as a goroutine)
// that scans the data source at regular intervals for pending campaigns,
// and launches independent goroutines for each campaign to handle sending.
// Each campaign now runs with its own dedicated concurrency and rate limiting.
func (m *Manager) Run() {
	if m.cfg.ScanCampaigns {
		// Periodically scan campaigns and launch independent processing for each
		go m.scanCampaigns(m.cfg.ScanInterval)
	}

	// Keep the manager running to handle campaign lifecycle
	select {}
}

// CacheTpl caches a template for ad-hoc use. This is currently only used by tx templates.
func (m *Manager) CacheTpl(id int, tpl *models.Template) {
	m.tplsMut.Lock()
	m.tpls[id] = tpl
	m.tplsMut.Unlock()
}

// DeleteTpl deletes a cached template.
func (m *Manager) DeleteTpl(id int) {
	m.tplsMut.Lock()
	delete(m.tpls, id)
	m.tplsMut.Unlock()
}

// GetTpl returns a cached template.
func (m *Manager) GetTpl(id int) (*models.Template, error) {
	m.tplsMut.RLock()
	tpl, ok := m.tpls[id]
	m.tplsMut.RUnlock()

	if !ok {
		return nil, fmt.Errorf("template %d not found", id)
	}

	return tpl, nil
}

// TemplateFuncs returns the template functions to be applied into
// compiled campaign templates.
func (m *Manager) TemplateFuncs(c *models.Campaign) template.FuncMap {
	f := template.FuncMap{
		"TrackLink": func(url string, msg *CampaignMessage) string {
			subUUID := msg.Subscriber.UUID
			if !m.cfg.IndividualTracking {
				subUUID = dummyUUID
			}

			return m.trackLink(url, msg.Campaign.UUID, subUUID)
		},
		"TrackView": func(msg *CampaignMessage) template.HTML {
			subUUID := msg.Subscriber.UUID
			if !m.cfg.IndividualTracking {
				subUUID = dummyUUID
			}

			return template.HTML(fmt.Sprintf(`<img src="%s" alt="" />`,
				fmt.Sprintf(m.cfg.ViewTrackURL, msg.Campaign.UUID, subUUID)))
		},
		"UnsubscribeURL": func(msg *CampaignMessage) string {
			return msg.unsubURL
		},
		"ManageURL": func(msg *CampaignMessage) string {
			return msg.unsubURL + "?manage=true"
		},
		"OptinURL": func(msg *CampaignMessage) string {
			// Add list IDs.
			// TODO: Show private lists list on optin e-mail
			return fmt.Sprintf(m.cfg.OptinURL, msg.Subscriber.UUID, "")
		},
		"MessageURL": func(msg *CampaignMessage) string {
			return fmt.Sprintf(m.cfg.MessageURL, c.UUID, msg.Subscriber.UUID)
		},
		"ArchiveURL": func() string {
			return m.cfg.ArchiveURL
		},
		"RootURL": func() string {
			return m.cfg.RootURL
		},
	}

	maps.Copy(f, m.tplFuncs)

	return f
}

func (m *Manager) GenericTemplateFuncs() template.FuncMap {
	return m.tplFuncs
}

// StopCampaign marks a running campaign as stopped so that all its queued messages are ignored.
func (m *Manager) StopCampaign(id int) {
	m.pipesMut.RLock()
	if p, ok := m.pipes[id]; ok {
		p.Stop(false)
	}
	m.pipesMut.RUnlock()
}

// Close closes and exits the campaign manager.
func (m *Manager) Close() {
	// Stop all running campaigns
	m.pipesMut.Lock()
	for _, p := range m.pipes {
		p.Stop(false)
	}
	m.pipesMut.Unlock()
}

// scanCampaigns is a blocking function that periodically scans the data source
// for campaigns to process and dispatches them to the manager. It feeds campaigns
// into nextPipes.
func (m *Manager) scanCampaigns(tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()

	// Periodically scan the data source for campaigns to process.
	for range t.C {
		ids, counts := m.getCurrentCampaigns()
		campaigns, err := m.store.NextCampaigns(ids, counts)
		if err != nil {
			m.log.Printf("error fetching campaigns: %v", err)
			continue
		}

		for _, c := range campaigns {
			// Create a new pipe that'll handle this campaign's states.
			p, err := m.newPipe(c)
			if err != nil {
				m.log.Printf("error processing campaign (%s): %v", c.Name, err)
				continue
			}
			m.log.Printf("start processing campaign (%s)", c.Name)

			// Launch an independent goroutine for this campaign
			go m.runCampaign(p)
		}
	}
}

// getCurrentCampaigns returns the IDs of campaigns currently being processed
// and their sent counts.
func (m *Manager) getCurrentCampaigns() ([]int64, []int64) {
	// Needs to return an empty slice in case there are no campaigns.
	m.pipesMut.RLock()
	defer m.pipesMut.RUnlock()

	var (
		ids    = make([]int64, 0, len(m.pipes))
		counts = make([]int64, 0, len(m.pipes))
	)
	for _, p := range m.pipes {
		ids = append(ids, int64(p.camp.ID))

		// Get the sent counts for campaigns and reset them to 0
		// as in the database, they're stored cumulatively (sent += $newSent).
		counts = append(counts, p.sent.Load())
		p.sent.Store(0)
	}

	return ids, counts
}

// trackLink register a URL and return its UUID to be used in message templates
// for tracking links.
func (m *Manager) trackLink(url, campUUID, subUUID string) string {
	url = strings.ReplaceAll(url, "&amp;", "&")

	m.linksMut.RLock()
	if uu, ok := m.links[url]; ok {
		m.linksMut.RUnlock()
		return fmt.Sprintf(m.cfg.LinkTrackURL, uu, campUUID, subUUID)
	}
	m.linksMut.RUnlock()

	// Register link.
	uu, err := m.store.CreateLink(url)
	if err != nil {
		m.log.Printf("error registering tracking for link '%s': %v", url, err)

		// If the registration fails, fail over to the original URL.
		return url
	}

	m.linksMut.Lock()
	m.links[url] = uu
	m.linksMut.Unlock()

	return fmt.Sprintf(m.cfg.LinkTrackURL, uu, campUUID, subUUID)
}

// sendNotif sends a notification to registered admin e-mails.
func (m *Manager) sendNotif(c *models.Campaign, status, reason string) error {
	var (
		subject = fmt.Sprintf("%s: %s", cases.Title(language.Und).String(status), c.Name)
		data    = map[string]any{
			"ID":     c.ID,
			"Name":   c.Name,
			"Status": status,
			"Sent":   c.Sent,
			"ToSend": c.ToSend,
			"Reason": reason,
		}
	)

	return m.fnNotify(subject, data)
}

// makeGnericFuncMap returns a generic template func map with custom template
// functions and sprig template functions.
func (m *Manager) makeGnericFuncMap() template.FuncMap {
	funcs := template.FuncMap{
		"Date": func(layout string) string {
			if layout == "" {
				layout = time.ANSIC
			}
			return time.Now().Format(layout)
		},
		"L": func() *i18n.I18n {
			return m.i18n
		},
		"Safe": func(safeHTML string) template.HTML {
			return template.HTML(safeHTML)
		},
	}

	// Copy spring functions.
	sprigFuncs := sprig.GenericFuncMap()
	delete(sprigFuncs, "env")
	delete(sprigFuncs, "expandenv")

	maps.Copy(funcs, sprigFuncs)

	return funcs
}

// attachMedia loads any media/attachments from the media store and attaches
// the byte blobs to the campaign.
func (m *Manager) attachMedia(c *models.Campaign) error {
	// Load any media/attachments.
	for _, mid := range []int64(c.MediaIDs) {
		a, err := m.store.GetAttachment(int(mid))
		if err != nil {
			return fmt.Errorf("error fetching attachment %d on campaign %s: %v", mid, c.Name, err)
		}

		c.Attachments = append(c.Attachments, a)
	}

	return nil
}

// MakeAttachmentHeader is a helper function that returns a
// textproto.MIMEHeader tailored for attachments, primarily
// email. If no encoding is given, base64 is assumed.
func MakeAttachmentHeader(filename, encoding, contentType string) textproto.MIMEHeader {
	if encoding == "" {
		encoding = "base64"
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", "attachment; filename="+filename)
	h.Set("Content-Type", fmt.Sprintf("%s; name=\""+filename+"\"", contentType))
	h.Set("Content-Transfer-Encoding", encoding)
	return h
}

// runCampaign runs a single campaign independently with its own rate limiting and concurrency.
// This function processes all subscribers for the given campaign.
func (m *Manager) runCampaign(p *pipe) {
	defer func() {
		// Clean up the pipe when done
		m.pipesMut.Lock()
		delete(m.pipes, p.camp.ID)
		m.pipesMut.Unlock()
		p.cleanup()
	}()

	// Launch workers for this specific campaign
	for i := 0; i < m.cfg.Concurrency; i++ {
		go m.campaignWorker(p)
	}

	// Process all batches for this campaign
	for {
		// Apply rate limiting with random delay before fetching next batch
		if m.cfg.RandomDelayMin > 0 && m.cfg.RandomDelayMax > 0 {
			minDelay := m.cfg.RandomDelayMin
			maxDelay := m.cfg.RandomDelayMax
			if maxDelay > minDelay {
				minDur := time.Duration(minDelay) * time.Second
				maxDur := time.Duration(maxDelay) * time.Second
				randomDelay := minDur + time.Duration(rand.Int63n(int64(maxDur-minDur)))
				
				m.log.Printf("batch rate limit: sleeping for %v before next batch (range %ds-%ds)", 
					randomDelay.Round(time.Second), minDelay, maxDelay)
				time.Sleep(randomDelay)
			}
		}
		
		has, err := p.NextSubscribers()
		if err != nil {
			m.log.Printf("error processing campaign batch (%s): %v", p.camp.Name, err)
			break
		}

		if !has {
			// No more subscribers, campaign is finished
			p.wg.Done()
			break
		}
	}
}

// sendMessage sends a campaign message using the appropriate messenger.
func (m *Manager) sendMessage(msg CampaignMessage) error {
	// If the campaign has ended or stopped, ignore the message.
	if msg.pipe != nil && msg.pipe.stopped.Load() {
		return nil
	}

	// Outgoing message.
	out := models.Message{
		From:        msg.from,
		To:          []string{msg.to},
		Subject:     msg.subject,
		ContentType: msg.Campaign.ContentType,
		Body:        msg.body,
		AltBody:     msg.altBody,
		Subscriber:  msg.Subscriber,
		Campaign:    msg.Campaign,
		Attachments: msg.Campaign.Attachments,
	}

	h := textproto.MIMEHeader{}
	h.Set(models.EmailHeaderCampaignUUID, msg.Campaign.UUID)
	h.Set(models.EmailHeaderSubscriberUUID, msg.Subscriber.UUID)

	// Attach List-Unsubscribe headers?
	if m.cfg.UnsubHeader {
		h.Set("List-Unsubscribe-Post", "List-Unsubscribe=One-Click")
		h.Set("List-Unsubscribe", `<`+msg.unsubURL+`>`)
	}

	// Attach any custom headers.
	if len(msg.Campaign.Headers) > 0 {
		for _, set := range msg.Campaign.Headers {
			for hdr, val := range set {
				h.Add(hdr, val)
			}
		}
	}

	// Set the headers.
	out.Headers = h

	// Push the message to the messenger.
	err := m.messengers[msg.Campaign.Messenger].Push(out)
	if err != nil {
		m.log.Printf("error sending message in campaign %s: subscriber %d: %v", msg.Campaign.Name, msg.Subscriber.ID, err)
	}

	// Increment the send rate or the error counter if there was an error.
	if msg.pipe != nil {
		if err != nil {
			// Call the error callback, which keeps track of the error count
			// and stops the campaign if the error count exceeds the threshold.
			msg.pipe.OnError()
		} else {
			id := uint64(msg.Subscriber.ID)
			if id > msg.pipe.lastID.Load() {
				msg.pipe.lastID.Store(uint64(msg.Subscriber.ID))
			}
			msg.pipe.rate.Incr(1)
			msg.pipe.sent.Add(1)
		}
	}

	return err
}

// campaignWorker is a worker that processes messages for a specific campaign.
func (m *Manager) campaignWorker(p *pipe) {
	// Counter to keep track of the message / sec rate limit.
	numMsg := 0
	for {
		select {
		case msg, ok := <-p.msgQ:
			if !ok {
				// Channel closed, campaign finished
				return
			}

			if msg.Campaign.Status != models.CampaignStatusRunning {
				// Campaign has been paused or cancelled.
				p.wg.Done()
				continue
			}

			// Throttle message sending according to the configured rate.
			if numMsg >= m.cfg.MessageRate {
				select {
				case <-time.After(time.Second):
					numMsg = 0
				}
			}

			// Send the message.
			err := m.sendMessage(msg)
			if err != nil {
				m.log.Printf("error sending message: %v", err)
			}

			numMsg++
			p.wg.Done()

		case <-time.After(time.Second):
			numMsg = 0
		}
	}
}
