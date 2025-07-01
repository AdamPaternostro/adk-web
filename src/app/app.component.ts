/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Component} from '@angular/core';
import {RouterOutlet} from '@angular/router';
import {SessionService} from './core/services/session.service';
import { MatIconRegistry } from '@angular/material/icon';
import { DomSanitizer } from '@angular/platform-browser';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss',
  standalone: false,
})
export class AppComponent {
  title = 'agent_framework_web';
  userId: string = '';
  appName: string = '';
  sessionId: string = '';

  constructor(
    private matIconRegistry: MatIconRegistry,
    private domSanitizer: DomSanitizer
  ) {
    // Register the custom icon
    // The first argument is the name you'll use in the template (e.g., 'robot_2_custom')
    // The second argument is the path to the SVG, sanitized for security
    this.matIconRegistry.addSvgIcon(
      'robot_2_custom', // The name for your icon
      this.domSanitizer.bypassSecurityTrustResourceUrl('../assets/icons/custom-robot.svg')
    );
  }
}
