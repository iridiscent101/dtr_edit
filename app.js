document.addEventListener("DOMContentLoaded", () => {
  const liveClock = document.getElementById("liveClock");
  const durationEl = document.getElementById("liveDuration");

  // 1. Handle Live Clock (Top of page)
  function updateClock() {
    if (liveClock) {
      liveClock.textContent = new Date().toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
      });
    }
  }
  setInterval(updateClock, 1000);

  // 2. Handle Live Work Duration (The "You have worked X hours" timer)
  function updateDuration() {
    if (durationEl && durationEl.dataset.startTime) {
      const startTime = parseInt(durationEl.dataset.startTime) * 1000; // Convert seconds to ms
      const now = Date.now();
      const diff = now - startTime;

      const hours = Math.floor(diff / 3600000);
      const minutes = Math.floor((diff % 3600000) / 60000);
      const seconds = Math.floor((diff % 60000) / 1000);

      durationEl.textContent = `${hours}h ${minutes}m ${seconds}s`;
    }
  }
  setInterval(updateDuration, 1000);
});
