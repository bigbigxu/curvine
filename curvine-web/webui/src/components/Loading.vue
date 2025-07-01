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
    <div className="loadingMessageWrapper">
        <div className="loadingMessage text-center">
            <img className="loadingMessageSpin" src="@/assets/curvine.svg" />
            <br />
            <h5 className="">Please wait...</h5>
        </div>
    </div>
</template>

<script>
export default {
    name: "HelloWorld",
    onMounted() {
        document.body.style.overflow = "hidden";
    },
    onUnmounted() {
        document.body.style.overflow = ""
    },
}
</script>

<style lang="scss">
@import "~bootswatch/dist/darkly/variables";

.loadingMessageWrapper {
    position: fixed;
    top: 33%;
    left: 50%;

    .loadingMessage {
        position: relative;
        left: -50%;
        padding: 1em 1.5em;
        color: $gray-400;
        background-color: $light;
        border-radius: 1em;

        img.loadingMessageSpin {
            height: 100px;
            width: 100px;
            -webkit-animation: loadingMessageSpin 2s ease-in-out infinite;
            -moz-animation: loadingMessageSpin 2s ease-in-out infinite;
            animation: loadingMessageSpin 2s ease-in-out infinite;
        }
    }

    @keyframes loadingMessageSpin {

        0%,
        50% {
            transform: rotateY(0deg);
        }

        50% {
            transform: rotateY(90deg);
        }

        100% {
            transform: rotateY(0deg);
        }
    }
}
</style>
