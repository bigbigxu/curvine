<!--
  - Copyright 2025 OPPO.
  -
  - Licensed under the Apache License, Version 2.0 (the "License");
  - you may not use this file except in compliance with the License.
  - You may obtain a copy of the License at
  -
  -     http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  -->

<template>
    <div v-loading="loading" class="workers">
        <!-- Live Workers Section -->
        <div class="workers-section">
            <div class="section-header">
                <div class="section-title">
                    <div class="title-icon">🟢</div>
                    <h2>Live Workers</h2>
                    <div class="worker-count">{{ data.live_workers ? data.live_workers.length : 0 }}</div>
                </div>
                <div class="section-stats">
                    <div class="stat-item">
                        <span class="stat-label">Total Capacity</span>
                        <span class="stat-value">{{ getTotalCapacity(data.live_workers) }}GB</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Available</span>
                        <span class="stat-value available">{{ getTotalAvailable(data.live_workers) }}GB</span>
                    </div>
                </div>
            </div>
            
            <div class="workers-grid" v-if="data.live_workers && data.live_workers.length > 0">
                <div v-for="worker in data.live_workers" :key="worker.address.worker_id" class="worker-card live">
                    <div class="worker-header">
                        <div class="worker-status">
                            <div class="status-indicator online">
                                <div class="pulse-dot"></div>
                            </div>
                            <div class="worker-id">{{ worker.address.worker_id }}</div>
                        </div>
                        <div class="worker-node">
                            <div class="node-name">{{ worker.address.hostname }}</div>
                            <div class="node-port">:{{ worker.address.rpc_port }}</div>
                        </div>
                    </div>
                    
                    <div class="worker-info">
                        <div class="info-row">
                            <span class="info-label">IP Address</span>
                            <span class="info-value">{{ worker.address.ip_addr }}</span>
                        </div>
                        <div class="info-row">
                            <span class="info-label">Last Heartbeat</span>
                            <span class="info-value">{{ formatTimestamp(worker.last_update) }}</span>
                        </div>
                    </div>
                    
                    <div class="storage-info">
                        <div class="storage-stats">
                            <div class="storage-stat">
                                <span class="storage-label">Capacity</span>
                                <span class="storage-value">{{ toGB(worker.capacity) }}GB</span>
                            </div>
                            <div class="storage-stat">
                                <span class="storage-label">Available</span>
                                <span class="storage-value available">{{ toGB(worker.available) }}GB</span>
                            </div>
                            <div class="storage-stat">
                                <span class="storage-label">Used</span>
                                <span class="storage-value used">{{ toGB(worker.fs_used) }}GB</span>
                            </div>
                        </div>
                        
                        <div class="usage-bar">
                            <div class="usage-track">
                                <div class="usage-fill" :style="{ width: getUsagePercentage(worker) + '%' }"></div>
                            </div>
                            <div class="usage-text">{{ getUsagePercentage(worker) }}% Used</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div v-else class="empty-state">
                <div class="empty-icon">📡</div>
                <div class="empty-text">No live workers found</div>
            </div>
        </div>
        
        <!-- Lost Workers Section -->
        <div class="workers-section lost-section">
            <div class="section-header">
                <div class="section-title">
                    <div class="title-icon">🔴</div>
                    <h2>Lost Workers</h2>
                    <div class="worker-count lost">{{ data.lost_workers ? data.lost_workers.length : 0 }}</div>
                </div>
                <div class="section-stats" v-if="data.lost_workers && data.lost_workers.length > 0">
                    <div class="stat-item">
                        <span class="stat-label">Lost Capacity</span>
                        <span class="stat-value lost">{{ getTotalCapacity(data.lost_workers) }}GB</span>
                    </div>
                </div>
            </div>
            
            <div class="workers-grid" v-if="data.lost_workers && data.lost_workers.length > 0">
                <div v-for="worker in data.lost_workers" :key="worker.address.worker_id" class="worker-card lost">
                    <div class="worker-header">
                        <div class="worker-status">
                            <div class="status-indicator offline"></div>
                            <div class="worker-id">{{ worker.address.worker_id }}</div>
                        </div>
                        <div class="worker-node">
                            <div class="node-name">{{ worker.address.hostname }}</div>
                            <div class="node-port">:{{ worker.address.rpc_port }}</div>
                        </div>
                    </div>
                    
                    <div class="worker-info">
                        <div class="info-row">
                            <span class="info-label">IP Address</span>
                            <span class="info-value">{{ worker.address.ip_addr }}</span>
                        </div>
                        <div class="info-row">
                            <span class="info-label">Last Heartbeat</span>
                            <span class="info-value">{{ formatTimestamp(worker.last_update) }}</span>
                        </div>
                    </div>
                    
                    <div class="storage-info">
                        <div class="storage-stats">
                            <div class="storage-stat">
                                <span class="storage-label">Capacity</span>
                                <span class="storage-value">{{ toGB(worker.capacity) }}GB</span>
                            </div>
                            <div class="storage-stat">
                                <span class="storage-label">Available</span>
                                <span class="storage-value">{{ toGB(worker.available) }}GB</span>
                            </div>
                            <div class="storage-stat">
                                <span class="storage-label">Used</span>
                                <span class="storage-value">{{ toGB(worker.fs_used) }}GB</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div v-else class="empty-state">
                <div class="empty-icon">✅</div>
                <div class="empty-text">No lost workers - All systems operational</div>
            </div>
        </div>
    </div>
</template>

<script>
import { fetchWorkersData } from '@/api/client'
import { toGB, formatTimestamp } from '@/utils/utils'
import eventBus from '@/utils/eventBus'

/* eslint-disable vue/multi-word-component-names */
export default {
    name: 'Workers',
    setup() {
        return {
            toGB,
            formatTimestamp
        }
    },
    data() {
        return {
            loading: false,
            data: {},
            live_workers: [],
            lost_workers: [],
        }
    },
    created() {
        this.loading = true
        this.fetchWorkersData()
        this.loading = false
    },
    mounted() {
        // Listen for auto refresh events
        eventBus.on('auto-refresh-trigger', this.handleAutoRefresh)
    },
    beforeUnmount() {
        // Clean up event listeners
        eventBus.off('auto-refresh-trigger', this.handleAutoRefresh)
    },
    methods: {
        /**
         * Handle auto refresh trigger from header component
         */
        handleAutoRefresh() {
            this.fetchWorkersData()
        },
        
        /**
         * Fetch workers data from API
         */
        fetchWorkersData() {
            fetchWorkersData().then(res => {
                this.data = res.data
            }).catch(err => {
                console.error("fetch browse data error: " + err)
            })
        },
        getTotalCapacity(workers) {
            if (!workers || workers.length === 0) return 0
            return this.toGB(workers.reduce((total, worker) => total + worker.capacity, 0))
        },
        getTotalAvailable(workers) {
            if (!workers || workers.length === 0) return 0
            return this.toGB(workers.reduce((total, worker) => total + worker.available, 0))
        },
        getUsagePercentage(worker) {
            if (!worker.capacity || worker.capacity === 0) return 0
            return Math.round((worker.fs_used / worker.capacity) * 100)
        }
    }
}
</script>

<style lang="scss" scoped>
.workers {
    padding: 2rem;
    background: var(--primary-bg);
    min-height: 100vh;
    
    .workers-section {
        margin-bottom: 3rem;
        
        &.lost-section {
            .section-header {
                border-left: 4px solid #ff6b6b;
            }
        }
        
        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            padding: 1.5rem;
            background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--card-bg) 100%);
            border-radius: 16px;
            border: 1px solid var(--border-color);
            border-left: 4px solid var(--accent-green);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            
            .section-title {
                display: flex;
                align-items: center;
                gap: 1rem;
                
                .title-icon {
                    font-size: 1.5rem;
                    filter: drop-shadow(0 0 8px currentColor);
                }
                
                h2 {
                    margin: 0;
                    font-size: 1.8rem;
                    font-weight: 700;
                    color: var(--text-primary);
                    font-family: 'JetBrains Mono', monospace;
                    letter-spacing: 1px;
                    text-transform: uppercase;
                }
                
                .worker-count {
                    background: var(--accent-blue);
                    color: white;
                    padding: 0.5rem 1rem;
                    border-radius: 20px;
                    font-weight: 700;
                    font-family: 'JetBrains Mono', monospace;
                    box-shadow: 0 0 15px rgba(0, 212, 255, 0.5);
                    
                    &.lost {
                        background: #ff6b6b;
                        box-shadow: 0 0 15px rgba(255, 107, 107, 0.5);
                    }
                }
            }
            
            .section-stats {
                display: flex;
                gap: 2rem;
                
                .stat-item {
                    text-align: right;
                    
                    .stat-label {
                        display: block;
                        font-size: 0.8rem;
                        color: var(--text-secondary);
                        text-transform: uppercase;
                        letter-spacing: 1px;
                        margin-bottom: 0.5rem;
                    }
                    
                    .stat-value {
                        display: block;
                        font-size: 1.5rem;
                        font-weight: 700;
                        color: var(--accent-blue);
                        font-family: 'JetBrains Mono', monospace;
                        
                        &.available {
                            color: var(--accent-green);
                        }
                        
                        &.lost {
                            color: #ff6b6b;
                        }
                    }
                }
            }
        }
        
        .workers-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
            gap: 1.5rem;
            
            .worker-card {
                background: var(--card-bg);
                border: 1px solid var(--border-color);
                border-radius: 16px;
                padding: 1.5rem;
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                transition: all 0.3s ease;
                position: relative;
                overflow: hidden;
                
                &::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    height: 4px;
                    background: var(--accent-green);
                    transition: all 0.3s ease;
                }
                
                &.lost::before {
                    background: #ff6b6b;
                }
                
                &:hover {
                    transform: translateY(-4px);
                    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
                    
                    &.live {
                        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4), 0 0 20px rgba(0, 255, 127, 0.3);
                    }
                    
                    &.lost {
                        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4), 0 0 20px rgba(255, 107, 107, 0.3);
                    }
                }
                
                .worker-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 1.5rem;
                    
                    .worker-status {
                        display: flex;
                        align-items: center;
                        gap: 1rem;
                        
                        .status-indicator {
                            position: relative;
                            width: 16px;
                            height: 16px;
                            border-radius: 50%;
                            
                            &.online {
                                background: var(--accent-green);
                                box-shadow: 0 0 15px var(--accent-green);
                                
                                .pulse-dot {
                                    position: absolute;
                                    top: 50%;
                                    left: 50%;
                                    transform: translate(-50%, -50%);
                                    width: 100%;
                                    height: 100%;
                                    border-radius: 50%;
                                    background: var(--accent-green);
                                    animation: pulse 2s infinite;
                                }
                            }
                            
                            &.offline {
                                background: #ff6b6b;
                                box-shadow: 0 0 15px #ff6b6b;
                            }
                        }
                        
                        .worker-id {
                            font-size: 1.1rem;
                            font-weight: 700;
                            color: var(--text-primary);
                            font-family: 'JetBrains Mono', monospace;
                        }
                    }
                    
                    .worker-node {
                        text-align: right;
                        
                        .node-name {
                            font-size: 1rem;
                            font-weight: 600;
                            color: var(--accent-blue);
                            font-family: 'JetBrains Mono', monospace;
                        }
                        
                        .node-port {
                            font-size: 0.9rem;
                            color: var(--text-secondary);
                            font-family: 'JetBrains Mono', monospace;
                        }
                    }
                }
                
                .worker-info {
                    margin-bottom: 1.5rem;
                    
                    .info-row {
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                        padding: 0.75rem 0;
                        border-bottom: 1px solid var(--border-color);
                        
                        &:last-child {
                            border-bottom: none;
                        }
                        
                        .info-label {
                            color: var(--text-secondary);
                            font-weight: 500;
                            text-transform: uppercase;
                            font-size: 0.8rem;
                            letter-spacing: 0.5px;
                        }
                        
                        .info-value {
                            color: var(--text-primary);
                            font-weight: 600;
                            font-family: 'JetBrains Mono', monospace;
                        }
                    }
                }
                
                .storage-info {
                    .storage-stats {
                        display: grid;
                        grid-template-columns: repeat(3, 1fr);
                        gap: 1rem;
                        margin-bottom: 1rem;
                        
                        .storage-stat {
                            text-align: center;
                            padding: 1rem;
                            background: var(--secondary-bg);
                            border-radius: 12px;
                            border: 1px solid var(--border-color);
                            
                            .storage-label {
                                display: block;
                                font-size: 0.7rem;
                                color: var(--text-secondary);
                                text-transform: uppercase;
                                letter-spacing: 0.5px;
                                margin-bottom: 0.5rem;
                            }
                            
                            .storage-value {
                                display: block;
                                font-size: 1.1rem;
                                font-weight: 700;
                                color: var(--text-primary);
                                font-family: 'JetBrains Mono', monospace;
                                
                                &.available {
                                    color: var(--accent-green);
                                }
                                
                                &.used {
                                    color: #ff6b6b;
                                }
                            }
                        }
                    }
                    
                    .usage-bar {
                        .usage-track {
                            height: 8px;
                            background: var(--border-color);
                            border-radius: 4px;
                            overflow: hidden;
                            margin-bottom: 0.5rem;
                            
                            .usage-fill {
                                height: 100%;
                                background: linear-gradient(90deg, var(--accent-blue) 0%, #ff6b6b 100%);
                                border-radius: 4px;
                                transition: width 0.5s ease;
                                box-shadow: 0 0 10px rgba(0, 212, 255, 0.5);
                            }
                        }
                        
                        .usage-text {
                            text-align: center;
                            font-size: 0.9rem;
                            color: var(--text-secondary);
                            font-weight: 600;
                            font-family: 'JetBrains Mono', monospace;
                        }
                    }
                }
            }
        }
        
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            
            .empty-icon {
                font-size: 4rem;
                margin-bottom: 1rem;
                filter: grayscale(0.7);
            }
            
            .empty-text {
                font-size: 1.2rem;
                color: var(--text-secondary);
                font-weight: 500;
            }
        }
    }
}

@keyframes pulse {
    0% {
        transform: translate(-50%, -50%) scale(1);
        opacity: 1;
    }
    100% {
        transform: translate(-50%, -50%) scale(2);
        opacity: 0;
    }
}
</style>