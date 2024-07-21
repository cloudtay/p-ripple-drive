<?php declare(strict_types=1);
/*
 * Copyright (c) 2023-2024.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * 特此免费授予任何获得本软件及相关文档文件（“软件”）副本的人，不受限制地处理
 * 本软件，包括但不限于使用、复制、修改、合并、出版、发行、再许可和/或销售
 * 软件副本的权利，并允许向其提供本软件的人做出上述行为，但须符合以下条件：
 *
 * 上述版权声明和本许可声明应包含在本软件的所有副本或主要部分中。
 *
 * 本软件按“原样”提供，不提供任何形式的保证，无论是明示或暗示的，
 * 包括但不限于适销性、特定目的的适用性和非侵权性的保证。在任何情况下，
 * 无论是合同诉讼、侵权行为还是其他方面，作者或版权持有人均不对
 * 由于软件或软件的使用或其他交易而引起的任何索赔、损害或其他责任承担责任。
 */

namespace Psc\Drive\ThinkPHP;

use P\System;
use think\console\Command;
use think\console\Input;
use think\console\input\Option;
use think\console\Output;

use function file_get_contents;
use function P\run;
use function putenv;
use function root_path;

use const PHP_BINARY;

class PDrive extends Command
{
    /**
     * @return void
     */
    protected function configure(): void
    {
        $this->setName('p-ripple')->setDescription('start the P-Drive service');
        $this->addOption('listen', 'l', Option::VALUE_OPTIONAL, 'listen', 'http://127.0.0.1:8008');
        $this->addOption('threads', 't', Option::VALUE_OPTIONAL, 'threads', 4);
    }

    /**
     * @param Input  $input
     * @param Output $output
     * @return void
     */
    protected function execute(Input $input, Output $output): void
    {
        $appPath = root_path();
        $listen  = $input->getOption('listen');
        $threads = $input->getOption('threads');

        putenv("P_RIPPLE_APP_PATH=$appPath");
        putenv("P_RIPPLE_LISTEN=$listen");
        putenv("P_RIPPLE_THREADS=$threads");

        $task = System::Process()->task(function () use ($listen) {
            $session = System::Proc()->open(PHP_BINARY);
            $session->input(file_get_contents(__DIR__ . '/Guide.php'));
            $session->inputEot();

            $session->onMessage = function ($data) {
                echo $data;
            };

            $session->onErrorMessage = function ($data) {
                echo $data;
            };

            $session->onClose = function () {
                echo 'Session closed.';
            };
        });

        $task->run();
        run();
    }
}
